package ai.platon.pulsar.rest.api.controller

import ai.platon.pulsar.common.Debug
import ai.platon.pulsar.rest.api.common.ScrapeResponseObjectMapper
import ai.platon.pulsar.rest.api.entities.ScrapeRequest
import ai.platon.pulsar.rest.api.entities.ScrapeResponse
import ai.platon.pulsar.rest.api.entities.ScrapeStatusRequest
import ai.platon.pulsar.rest.api.entities.ScrapeRequestSubmitResponse
import ai.platon.pulsar.rest.api.service.ScrapeService
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import javax.servlet.http.HttpServletRequest

@RestController
@CrossOrigin
@RequestMapping(
    "x",
    consumes = [MediaType.TEXT_PLAIN_VALUE, "${MediaType.TEXT_PLAIN_VALUE};charset=UTF-8", MediaType.APPLICATION_JSON_VALUE],
    produces = [MediaType.APPLICATION_JSON_VALUE]
)
class ScrapeController(
    val applicationContext: ApplicationContext,
    val scrapeService: ScrapeService,
) {
    private val logger = LoggerFactory.getLogger(this.javaClass)

    /**
     * @param sql The sql to execute
     * @return The response
     * */
    @PostMapping("e")
    fun execute(@RequestBody sql: String): ScrapeResponse {
//        TODO("Not implemented yet, ScrapeResponse serialize will be a problem if not use ScrapeResponseObjectMapper")
        logger.debug("Execute sql: {}", sql)
        return scrapeService.executeQuery(ScrapeRequest(sql))
    }

    @PostMapping("e_json_async")
    fun execute(@RequestBody scrapeRequest: ScrapeRequest): ScrapeRequestSubmitResponse {
        logger.debug("{}. Execute scrapeRequest: {}", Debug.getLineInfo(), scrapeRequest)
        val submit = scrapeService.submitJob(scrapeRequest)
        return ScrapeRequestSubmitResponse(submit)
    }

    /**
     * @param sql The sql to execute
     * @return The uuid of the scrape task
     * */
    @PostMapping("s")
    fun submitJob(@RequestBody sql: String): String {
        logger.debug("{}. request: {}", Debug.getLineInfo(), sql)
        return scrapeService.submitJob(ScrapeRequest(sql))
    }

    /**
     * @param uuid The uuid of the task last submitted
     * @return The execution result
     * */
    @GetMapping("status", consumes = [MediaType.ALL_VALUE])
    fun status(
        @RequestParam uuid: String,
        httpRequest: HttpServletRequest,
    ): ScrapeResponse {
        val request = ScrapeStatusRequest(uuid)
        return scrapeService.getStatus(request)
    }
}
