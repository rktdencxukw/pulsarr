package ai.platon.pulsar.rest.api.common

import ai.platon.pulsar.session.PulsarSession
import ai.platon.pulsar.common.*
import ai.platon.pulsar.common.PulsarParams.VAR_IS_SCRAPE
import ai.platon.pulsar.crawl.event.impl.DefaultLoadEvent
import ai.platon.pulsar.crawl.event.impl.DefaultPageEvent
import ai.platon.pulsar.crawl.PageEvent
import ai.platon.pulsar.crawl.common.GlobalCacheFactory
import ai.platon.pulsar.crawl.common.url.CompletableListenableHyperlink
import ai.platon.pulsar.dom.FeaturedDocument
import ai.platon.pulsar.persist.WebPage
import ai.platon.pulsar.ql.context.AbstractSQLContext
import ai.platon.pulsar.ql.ResultSets
import ai.platon.pulsar.ql.h2.utils.ResultSetUtils
import ai.platon.pulsar.rest.api.entities.ScrapeRequest
import ai.platon.pulsar.rest.api.entities.ScrapeResponse
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.google.gson.Gson
import org.h2.jdbc.JdbcSQLException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.sql.Connection
import java.sql.ResultSet
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.system.measureTimeMillis
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer
import com.google.gson.annotations.SerializedName
import org.h2.jdbc.JdbcArray
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter


class Markdown(val content: String) {
}

class WeChatMarkdownMsg(val title: String, val content: String) {
    @SerializedName("msgtype")
    val msgType = "markdown"
    lateinit var markdown: Markdown

    init {
        val content = """
           ${DateTimes.now()}\n
           title: ${title}\n\n
           content: $content
       """
        markdown = Markdown(content)
    }
}

class ScrapeLoadEvent(
    val hyperlink: XSQLScrapeHyperlink,
    val response: ScrapeResponse,
) : DefaultLoadEvent() {
    init {
        onWillLoad.addLast {
            response.pageStatusCode = ResourceStatus.SC_PROCESSING
            null
        }
        onWillParseHTMLDocument.addLast { page ->
            page.variables[VAR_IS_SCRAPE] = true
            null
        }
        onWillParseHTMLDocument.addLast { page ->
        }
        onHTMLDocumentParsed.addLast { page, document -> // kcread 完成回调 5. parsed，准备执行xql抽取。extract阶段会再次通过http获取页面，如果有缓存可能不会？
            require(page.hasVar(VAR_IS_SCRAPE))
            hyperlink.extract(page, document)
        }
        onLoaded.addLast { page ->
            hyperlink.complete(page)
        }
    }
}

class ReportHttpClient {
    companion object {
        @JvmStatic
        val instance: HttpClient = HttpClient.newHttpClient()
    }
}


private class MyInstantSerializer
    : InstantSerializer(InstantSerializer.INSTANCE, false, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(TimeZone.getTimeZone("GMT+8:00").toZoneId()))
private class MyInstantDeserializer
    : InstantDeserializer<Instant>(InstantDeserializer.INSTANT, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(TimeZone.getTimeZone("GMT+8:00").toZoneId()))


class ScrapeResponseObjectMapper {
    companion object {
        @JvmStatic
        val instance: ObjectMapper = ObjectMapper().apply {
            val jm = JavaTimeModule()
            jm.addSerializer(Instant::class.java, MyInstantSerializer())
            jm.addDeserializer(Instant::class.java, MyInstantDeserializer())
            registerModule(jm)
            registerModule(SimpleModule().apply {
                addSerializer(JdbcArray::class.java, JdbcArraySerializer())
            })
            setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
            configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            configure(SerializationFeature.INDENT_OUTPUT, true);
            configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            setTimeZone(TimeZone.getTimeZone("GMT+8:00"))
            dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        }
    }
}

open class XSQLScrapeHyperlink(
    val request: ScrapeRequest,
    val sql: NormXSQL,
    val session: PulsarSession,
    val globalCacheFactory: GlobalCacheFactory,
    val uuid: String = UUID.randomUUID().toString()
) : CompletableListenableHyperlink<ScrapeResponse>(sql.url) {

    private val logger = getLogger(XSQLScrapeHyperlink::class)

    private val sqlContext get() = session.context as AbstractSQLContext
    private val connectionPool get() = sqlContext.connectionPool
    private val randomConnection get() = sqlContext.randomConnection

    val response = ScrapeResponse()
    private val reportHttpClient = HttpClient.newHttpClient()

    override var args: String? = "-parse ${sql.args}"
    override var event: PageEvent = DefaultPageEvent(
        loadEvent = ScrapeLoadEvent(this, response)
    )

    open fun executeQuery(): ResultSet = executeQuery(request, response)

    open fun extract(page: WebPage, document: FeaturedDocument) {
        try {
            response.pageContentBytes = page.contentLength.toInt()
            response.pageStatusCode = page.protocolStatus.minorCode

            doExtract(page, document)
        } catch (t: Throwable) {
            logger.warn("Unexpected exception", t)
        }
    }

    //kcread 爬取完成会及时回调 complete
    open fun complete(page: WebPage) {
        response.uuid = uuid
        response.isDone = true
        response.finishTime = Instant.now()

        complete(response)

        if (!request.reportUrl.isNullOrEmpty()) {
            var tried = 0
            while (++tried <= 3) {
                try {
                    logger.info("Scrape task completed: ${response.uuid}")
                    val request = createPostRequest(request.reportUrl, response)
                    val response = ReportHttpClient.instance.send(request, HttpResponse.BodyHandlers.ofString())
                    logger.debug("report scrape result: {}", response)
                    if (response.statusCode() == 200) {
                        break
                    } else {
                        logger.error("report scrape result failed: {}", response)
                    }
                } catch (e: Exception) {
                    logger.error(
                        "failed to report scrape result. exception:{}, exception msg: {}, request:{}, response:{}",
                        e,
                        e.message,
                        request,
                        response
                    )
                    var msg = WeChatMarkdownMsg("report failed", e.message.toString())
                    var gson = Gson()
                    val request = HttpRequest.newBuilder()
                        .uri(URI.create("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=5932e314-7ffe-47bd-a097-87e9a39af354"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(msg))).build()
                    reportHttpClient.send(request, HttpResponse.BodyHandlers.ofString())
                }
                sleep(Duration.ofSeconds(1))
            }
        }
    }

    private fun createPostRequest(url: String, requestEntity: ScrapeResponse): HttpRequest {
        val requestBody = ScrapeResponseObjectMapper.instance.writeValueAsString(requestEntity)
        return HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build()
    }

    protected open fun doExtract(page: WebPage, document: FeaturedDocument): ResultSet {
        if (!page.protocolStatus.isSuccess ||
            page.contentLength == 0L || page.persistedContentLength == 0L
            || page.content == null
        ) {
            response.statusCode = ResourceStatus.SC_NO_CONTENT
            return ResultSets.newSimpleResultSet()
        }

        return executeQuery(request, response)
    }

    protected open fun executeQuery(request: ScrapeRequest, response: ScrapeResponse): ResultSet {
        var rs: ResultSet = ResultSets.newSimpleResultSet()

        try {
            response.statusCode = ResourceStatus.SC_OK

            rs = executeQuery(sql.sql)
            val resultSet = mutableListOf<Map<String, Any?>>()
            ResultSetUtils.getEntitiesFromResultSetTo(rs, resultSet)
            response.resultSet = resultSet
        } catch (e: JdbcSQLException) {
            response.statusCode = ResourceStatus.SC_EXPECTATION_FAILED
            logger.warn("Failed to execute sql #${response.uuid}{}", e.brief())
        } catch (e: Throwable) {
            response.statusCode = ResourceStatus.SC_EXPECTATION_FAILED
            logger.warn("Failed to execute sql #${response.uuid}\n{}", e.brief())
        }

        return rs
    }

    private fun executeQuery(sql: String): ResultSet {
//        return session.executeQuery(sql)
        val connection = connectionPool.poll() ?: randomConnection
        return executeQuery(sql, connection).also { connectionPool.offer(connection) }
    }

    private fun executeQuery(sql: String, conn: Connection): ResultSet {
        var result: ResultSet? = null
        val millis = measureTimeMillis {
            conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)?.use { st ->
                try {
                    st.executeQuery(sql)?.use { rs ->
                        result = ResultSetUtils.copyResultSet(rs)
                    }
                } catch (e: JdbcSQLException) {
                    val message = e.toString()
                    if (message.contains("Syntax error in SQL statement")) {
                        response.statusCode = ResourceStatus.SC_BAD_REQUEST
                        logger.warn("Syntax error in SQL statement #${response.uuid}>>>\n{}\n<<<", e.sql)
                    } else {
                        response.statusCode = ResourceStatus.SC_EXPECTATION_FAILED
                        logger.warn("Failed to execute scrape task #${response.uuid}\n{}", e.stringify())
                    }
                }
            }
        }

        return result ?: ResultSets.newResultSet()
    }
}

