package ai.platon.pulsar.rest.api.controller

import ai.platon.pulsar.crawl.fetch.privacy.PrivacyManager
import ai.platon.pulsar.protocol.browser.driver.WebDriverPoolManager
import ai.platon.pulsar.protocol.browser.emulator.context.MultiPrivacyContextManager
import ai.platon.pulsar.rest.api.service.LoadService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*

/**
 * The controller to handle www resources
 * */
@RestController
@CrossOrigin
@RequestMapping("pulsar-system")
class PulsarSystemController {

    @Autowired
    lateinit var driverPoolManager: WebDriverPoolManager

    @Autowired
    lateinit var privacyManager: PrivacyManager

    @GetMapping("hello")
    fun hello(): String {
        return "hello"
    }

    @GetMapping("report")
    fun report(): String {
        val sb = StringBuilder()
        sb.appendLine("Pulsar system reporting")
        sb.appendLine(driverPoolManager.takeSnapshot(true))
        sb.appendLine().appendLine()
        sb.appendLine(privacyManager.takeSnapshot())
        return sb.toString()
    }
}
