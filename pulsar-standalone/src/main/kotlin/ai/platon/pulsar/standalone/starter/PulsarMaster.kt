package ai.platon.pulsar.standalone.starter

import ai.platon.pulsar.boot.autoconfigure.PulsarContextInitializer
import ai.platon.pulsar.common.websocket.ScrapeNodeRegisterInfo
import ai.platon.pulsar.common.metrics.AppMetrics
import ai.platon.pulsar.common.websocket.Command
import ai.platon.pulsar.common.websocket.CommandResponse
import ai.platon.pulsar.common.websocket.ExoticResponse
import ai.platon.pulsar.crawl.CrawlLoops
import ai.platon.pulsar.persist.metadata.FetchMode
import ai.platon.pulsar.persist.metadata.IpType
import ai.platon.pulsar.rest.api.common.JdbcArraySerializer
import ai.platon.pulsar.rest.api.entities.ScrapeRequest
import ai.platon.pulsar.rest.api.entities.ScrapeRequestSubmitResponse
import ai.platon.pulsar.rest.api.service.ScrapeService
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer
import com.fasterxml.jackson.module.kotlin.readValue
import org.h2.jdbc.JdbcArray
import org.h2.tools.Server
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.ImportResource
import org.springframework.context.support.AbstractApplicationContext
import org.springframework.core.env.Environment
import org.springframework.messaging.converter.MappingJackson2MessageConverter
import org.springframework.messaging.simp.stomp.StompCommand
import org.springframework.messaging.simp.stomp.StompHeaders
import org.springframework.messaging.simp.stomp.StompSession
import org.springframework.messaging.simp.stomp.StompSessionHandler
import org.springframework.web.socket.client.WebSocketClient
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import org.springframework.web.socket.messaging.WebSocketStompClient
import java.io.File
import java.lang.reflect.Type
import java.sql.SQLException
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.*
import javax.annotation.PostConstruct


@SpringBootApplication
@ImportResource("classpath:pulsar-beans/app-context.xml")
@ComponentScan("ai.platon.pulsar.rest.api")
class PulsarMaster(
    /**
     * Activate crawl loops
     * */
    val crawlLoops: CrawlLoops,
    val applicationContext: AbstractApplicationContext,
    val env: Environment,
    val scrapeService: ScrapeService
) {
    private val logger = LoggerFactory.getLogger(PulsarMaster::class.java)

    @Autowired
    private val appMetrics: AppMetrics? = null

    var client: WebSocketClient = StandardWebSocketClient()
    private val stompClient = WebSocketStompClient(client)

    //    lateinit var sessionHandler: StompSessionHandler
    lateinit var stompSession: StompSession
    private val sessionHandler = CustomStompSessionHandler(env,scrapeService, ohObjectMapper())

    /**
     * Enable H2 client
     * */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @Throws(SQLException::class)
    fun h2Server(): Server {
        // return Server.createTcpServer("-trace")
        return Server.createTcpServer()
    }

    /**
     * Enable H2 console
     * */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @Throws(SQLException::class)
    fun h2WebServer(): Server {
        return Server.createWebServer("-webAllowOthers")
    }

//    @Bean
    fun ohObjectMapper(): ObjectMapper {
        val mapper = ObjectMapper()
        val jm = JavaTimeModule()
        jm.addSerializer(Instant::class.java, MyInstantSerializer())
        jm.addDeserializer(Instant::class.java, MyInstantDeserializer())
        mapper.registerModule(jm)
        mapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.setTimeZone(TimeZone.getTimeZone("GMT+8:00"))
        mapper.dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        return mapper
    }

    @PostConstruct
    fun postConstruct() {
        logger.info("PostConstruct Pulsar master is running")
        HttpsUrlValidator.disableSSLVerified()

        val idFile = File(".pulsar-node-id")
        var nodeId = ""
        if (idFile.exists()) {
            nodeId = idFile.readText()
            logger.info("node id: $nodeId")
        }
        val exoticServer = env.getProperty("pulsar.exotic.server", "")


        if (exoticServer.isNullOrEmpty().not()) {
            logger.info("connection exotic master via weboscket")
            var mm = MappingJackson2MessageConverter()
            mm.objectMapper = ohObjectMapper()
            stompClient.messageConverter = mm
            stompSession = stompClient.connect(
                "ws://$exoticServer/exotic/ws",
                sessionHandler
            ).get()
            sessionHandler.thisSession = stompSession
        }
    }
}


private class MyInstantSerializer
    : InstantSerializer(InstantSerializer.INSTANCE, false, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(TimeZone.getTimeZone("GMT+8:00").toZoneId()))
private class MyInstantDeserializer
    : InstantDeserializer<Instant>(InstantDeserializer.INSTANT, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(TimeZone.getTimeZone("GMT+8:00").toZoneId()))


class CustomStompSessionHandler(
    val env: Environment,
    val scrapeService: ScrapeService,
    val ohObjectMapper: ObjectMapper
) : StompSessionHandler {

    private val proxy = env.getProperty("pulsar.proxy", "")
    private val logger = LoggerFactory.getLogger(CustomStompSessionHandler::class.java)
    lateinit var thisSession: StompSession

    private val scrapeResponseObjectMapper: ObjectMapper = ObjectMapper().apply {
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
    override fun getPayloadType(headers: StompHeaders): Type {
        println("getPayloadType, $headers")
//        return String::class.java
//        return String::class.java
//        if (headers["oh_action"] != null) {
//            val action = headers["oh_action"][0] as String
//            when(action) {
//                "scrape" -> return Command::class.java
//            }
//        }
        return ExoticResponse::class.java // works. 服务端必须返回 ExoticResponse 结构体
    }

    override fun handleFrame(headers: StompHeaders, payload: Any?) {
//        val msg: String = payload as String
        println("handleFrame, $headers, $payload")
        when (headers.destination) {
            "/topic/public" -> {
                logger.debug("Received : $payload, $headers")
                println("Received : $payload, $headers")
            }

            "/user/queue/command" -> {
                when (headers["oh_action"][0] as String) {
                    "scrape" -> {
                        val pl = payload as ExoticResponse<String>
                        val command = ohObjectMapper.readValue<Command<ScrapeRequest>>(pl.data!!)
                        val reqId = command!!.reqId
                        val serverTaskIds = command!!.args?.map { arg ->
                            println("scrape: $arg")
//                            "-proxyServer", "--proxy-server"
                            if (proxy.isNotEmpty()) {
//                                val sql = arg.sql.replace("-proxyServer ", "--proxy-server")
                               // regex to remove '-proxyServer 192.168.293.123:292'
                                println("sql before replace: ${arg.sql}")
                                var sql = arg.sql.replace("-proxyServer http://[0-9]+.[0-9]+.[0-9]+.[0-9]+:[0-9]+".toRegex(), "-proxyServer $proxy")
                                sql = sql.replace("--proxy-server http://[0-9]+.[0-9]+.[0-9]+.[0-9]+:[0-9]+".toRegex(),  "-proxyServer $proxy")
                                arg.sql = sql
                                arg.sql = if (arg.sql.contains("-proxyServer")) {
                                    arg.sql.replace("-proxyServer http://[0-9]+.[0-9]+.[0-9]+.[0-9]+:[0-9]+".toRegex(), "-proxyServer $proxy")
                                } else if (arg.sql.contains("--proxy-server")) {
                                    arg.sql.replace("--proxy-server http://[0-9]+.[0-9]+.[0-9]+.[0-9]+:[0-9]+".toRegex(),  "-proxyServer $proxy")
                                } else {
                                    arg.sql.replace("(-taskId [0-9a-zA-Z]+ )".toRegex(),  "$1 -proxyServer $proxy")
                                }
                                println("sql after replace: ${arg.sql}")
                            }

                            val serverTaskId = scrapeService.submitJob(arg, reportHandler = { res ->
                                thisSession.send("/app/scrape_task_finished", scrapeResponseObjectMapper.writeValueAsString(res))
                            })
                            println("submit result: $serverTaskId")
                            var rsp = CommandResponse<ScrapeRequestSubmitResponse>(reqId)
                            rsp.data = ScrapeRequestSubmitResponse(serverTaskId)
                            thisSession.send("/app/scrape_task_submitted", scrapeResponseObjectMapper.writeValueAsString(rsp))
                            serverTaskId
                        } ?: listOf()
                    }
                }
            }

            "/user/queue/scrape_register" -> {
                val msg = payload as ExoticResponse<String>
                val nodeId = msg.data
                if (msg.code != 0L) {
                    println("scrape_register failed: $nodeId")
                } else {
                    println("scrape_register success: $nodeId")
                }
            }
        }
        println("Received : $payload, $headers")
    }

    override fun afterConnected(session: StompSession, connectedHeaders: StompHeaders) {
        logger.info("Connected to exotic server")
        session.subscribe("/user/queue/scrape_register", this)
        session.subscribe("/user/queue/command", this)
        val ipType = env.getProperty("pulsar.ip.type", IpType.SERVER.name) // residence, server
        val fetchModeSupport =
            env.getProperty("pulsar.fetch_mode.support", FetchMode.BROWSER.name) // headless, browser, native
        var node = ScrapeNodeRegisterInfo("", ipType, fetchModeSupport)
        session.send("/app/scrape_register", node)
    }

    override fun handleException(
        session: StompSession,
        command: StompCommand?,
        headers: StompHeaders,
        payload: ByteArray,
        exception: Throwable
    ) {
        logger.error("handleException, $headers, $payload, $exception, $command, $session")
    }

    override fun handleTransportError(session: StompSession, exception: Throwable) {
        logger.error("handleTransportError, $exception, $session")
    }
}


fun main(args: Array<String>) {
    runApplication<PulsarMaster>(*args) {
        addInitializers(PulsarContextInitializer())
        setAdditionalProfiles("master")
        setLogStartupInfo(true)
    }
}
