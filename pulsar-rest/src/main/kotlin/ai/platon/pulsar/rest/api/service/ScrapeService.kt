package ai.platon.pulsar.rest.api.service

import ai.platon.pulsar.session.PulsarSession
import ai.platon.pulsar.common.ResourceStatus
import ai.platon.pulsar.crawl.common.GlobalCacheFactory
import ai.platon.pulsar.persist.metadata.ProtocolStatusCodes
import ai.platon.pulsar.rest.api.common.DegenerateXSQLScrapeHyperlink
import ai.platon.pulsar.rest.api.common.ScrapeAPIUtils
import ai.platon.pulsar.rest.api.common.XSQLScrapeHyperlink
import ai.platon.pulsar.rest.api.entities.ScrapeRequest
import ai.platon.pulsar.rest.api.entities.ScrapeResponse
import ai.platon.pulsar.rest.api.entities.ScrapeStatusRequest
import com.google.gson.Gson
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.TimeUnit

@Service
class ScrapeService(
    val session: PulsarSession,
    val globalCacheFactory: GlobalCacheFactory,
) {
    private val logger = LoggerFactory.getLogger(ScrapeService::class.java)
    private val responseCache = ConcurrentSkipListMap<String, ScrapeResponse>()
    private val urlPool get() = globalCacheFactory.globalCache.urlPool

    private val httpClient = HttpClient.newHttpClient()

    /**
     * Execute a scrape task and wait until the execution is done,
     * for test purpose only, no customer should access this api
     * */
    //kcread 启动入口 提交任务。   // spring rest 是一个线程，成本有点高，应及时返回。
    fun executeQuery(request: ScrapeRequest): ScrapeResponse {
        val hyperlink = createScrapeHyperlink(request)
        urlPool.higher3Cache.reentrantQueue.add(hyperlink)
        // 可能跟complete函数不在同一个线程，不被执行
//        hyperlink.whenComplete { scrapeResponse: ScrapeResponse, throwable: Throwable ->
//            if (throwable != null) {
//                logger.error(throwable.message, throwable)
//            } else {
//                logger.info("Scrape task completed: ${scrapeResponse.uuid}")
//            }
//        }
        return  hyperlink.get(3, TimeUnit.MINUTES)
    }

    /**
     * Submit a scraping task
     * */
    fun submitJob(request: ScrapeRequest): String {
        val hyperlink = createScrapeHyperlink(request)
//        if (!request.reportUrl.isNullOrEmpty()) {
//            hyperlink.whenComplete { scrapeResponse: ScrapeResponse, throwable: Throwable ->
//                if (throwable != null) {
//                    logger.error(throwable.message, throwable)
//                } else {
//                    logger.info("Scrape task completed: ${scrapeResponse.uuid}")
//                    val request = post(request.reportUrl, scrapeResponse)
//                    try {
//                        httpClient.send(request, HttpResponse.BodyHandlers.ofString())
//                    } catch (e: Exception) {
//                        logger.error("failed to report scrape result:", e.message, e)
//                    }
//                }
//            }
//        }
        responseCache[hyperlink.uuid] = hyperlink.response
        urlPool.normalCache.reentrantQueue.add(hyperlink)
        return hyperlink.uuid
    }

    private fun post(url: String, requestEntity: Any): HttpRequest {
        val requestBody = Gson().toJson(requestEntity)
        return HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofMinutes(3))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build()
    }

    /**
     * Get the response
     * */
    fun getStatus(request: ScrapeStatusRequest): ScrapeResponse {
        val resp = responseCache.get(request.uuid)
        return if (resp == null) {
            ScrapeResponse(request.uuid, ResourceStatus.SC_NOT_FOUND, ProtocolStatusCodes.NOT_FOUND)
        } else {
            if (resp.isDone) {
                GlobalScope.launch {
                    launch {
                        delay(1000 * 60 * 5)
                        responseCache.remove(request.uuid)
                    }
                }
            }
            resp;
        }
    }

    private fun createScrapeHyperlink(request: ScrapeRequest): XSQLScrapeHyperlink {
        val sql = request.sql
        return if (ScrapeAPIUtils.isScrapeUDF(sql)) {
            val xSQL = ScrapeAPIUtils.normalize(sql)
            XSQLScrapeHyperlink(request, xSQL, session, globalCacheFactory)
        } else {
            DegenerateXSQLScrapeHyperlink(request, session, globalCacheFactory)
        }
    }

    fun clean(uuid: String) {
        responseCache.remove(uuid)
    }
}
