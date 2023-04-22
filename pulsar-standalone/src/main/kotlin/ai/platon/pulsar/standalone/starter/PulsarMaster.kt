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
import ai.platon.pulsar.rest.api.entities.ScrapeRequest
import ai.platon.pulsar.rest.api.entities.ScrapeRequestSubmitResponse
import ai.platon.pulsar.rest.api.service.ScrapeService
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
import org.springframework.stereotype.Component
import org.springframework.web.socket.client.WebSocketClient
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import org.springframework.web.socket.messaging.WebSocketStompClient
import java.io.File
import java.lang.reflect.Type
import java.sql.SQLException
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
    private val sessionHandler = CustomStompSessionHandler(env,scrapeService)

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


        stompClient.messageConverter = MappingJackson2MessageConverter()
        stompSession = stompClient.connect(
            "ws://$exoticServer/exotic/ws",
            sessionHandler
        ).get()
        sessionHandler.thisSession = stompSession
    }
}

class ChatMessage {
    var type: MessageType? = null
    var content: String? = null
    var sender: String? = null

    enum class MessageType {
        CHAT, JOIN, LEAVE
    }
}

class CustomStompSessionHandler(
    val env: Environment,
    val scrapeService: ScrapeService
) : StompSessionHandler {

    private val proxy = env.getProperty("pulsar.proxy", "")
    private val logger = LoggerFactory.getLogger(CustomStompSessionHandler::class.java)
    lateinit var thisSession: StompSession
    override fun getPayloadType(headers: StompHeaders): Type {
        println("getPayloadType, $headers")
//        return String::class.java
        headers["oh_action"]?.let {
            return when (it as String) {
                "scrape" -> Command::class.java
                else -> throw RuntimeException("Unknown action: $it")
            }
        }
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
                when (headers["oh_action"] as String) {
                    "scrape" -> {
                        val command = payload as Command<ScrapeRequest>
                        val reqId = command!!.reqId
                        val serverTaskIds = command!!.args?.map { arg ->
                            println("scrape: $arg")
//                            "-proxyServer", "--proxy-server"
                            if (proxy.isNotEmpty()) {
//                                val sql = arg.sql.replace("-proxyServer ", "--proxy-server")
                               // regex to remove '-proxyServer 192.168.293.123:292'
                                println("sql before replace: ${arg.sql}")
                                var sql = arg.sql.replace("-proxyServer [0-9]+.[0-9]+.[0-9]+.[0-9]+:[0-9]+".toRegex(), "")
                                sql = sql.replace("--proxy-server [0-9]+.[0-9]+.[0-9]+.[0-9]+:[0-9]+".toRegex(), "")
                                arg.sql = "$sql -proxyServer $proxy"
                                println("sql after replace: ${arg.sql}")
                            }

                            val serverTaskId = scrapeService.submitJob(arg, reportHandler = { res ->
                                thisSession.send("/app/scrape_task_finished", res)
                            })
                            println("submit result: $serverTaskId")
                            var rsp = CommandResponse<ScrapeRequestSubmitResponse>(reqId)
                            rsp.data = ScrapeRequestSubmitResponse(serverTaskId)
                            thisSession.send("/app/scrape_task_submitted", rsp)
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
