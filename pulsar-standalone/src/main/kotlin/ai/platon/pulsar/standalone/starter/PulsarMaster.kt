package ai.platon.pulsar.standalone.starter

import ai.platon.pulsar.boot.autoconfigure.PulsarContextInitializer
import ai.platon.pulsar.common.metrics.AppMetrics
import ai.platon.pulsar.crawl.CrawlLoops
import org.h2.tools.Server
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.ImportResource
import org.springframework.context.support.AbstractApplicationContext
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
    val applicationContext: AbstractApplicationContext
) {
    private val logger = LoggerFactory.getLogger(PulsarMaster::class.java)

    @Autowired
    private val appMetrics: AppMetrics? = null

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
//        for(beanName in applicationContext.beanDefinitionNames) {
//            val aliases = applicationContext.getAliases(beanName)
//            logger.info("kcdebug. beanName:$beanName,别名:[${aliases.joinToString(",")}]")
//        }
//        appMetrics!!.start()
    }
}

fun main(args: Array<String>) {
    runApplication<PulsarMaster>(*args) {
        addInitializers(PulsarContextInitializer())
        setAdditionalProfiles("master")
        setLogStartupInfo(true)
    }
}
