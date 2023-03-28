package ai.platon.pulsar.rest.api

import ai.platon.pulsar.common.stringify
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Component
class AppStartupRunner : ApplicationRunner {
    @Throws(Exception::class)
    override fun run(args: ApplicationArguments) {
        logger.info(
            "Application started with option names : {}",
            args.optionNames
        )
        logger.info("Increment counter")
        counter++
        try {
            val hc = HttpClient.newHttpClient()
            val request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:8080"))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString("w='App Started'")).build()
            hc.send(request, HttpResponse.BodyHandlers.ofString())
        } catch (e: Exception) {
            logger.error(e.stringify())
        }
    }

    companion object {
        var logger: Logger = LoggerFactory.getLogger(AppStartupRunner::class.java)
        var counter = 0
    }
}