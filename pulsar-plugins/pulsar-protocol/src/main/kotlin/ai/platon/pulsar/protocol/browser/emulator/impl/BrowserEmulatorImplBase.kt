package ai.platon.pulsar.protocol.browser.emulator.impl

import ai.platon.pulsar.browser.common.BrowserSettings
import ai.platon.pulsar.browser.common.InteractSettings
import ai.platon.pulsar.common.*
import ai.platon.pulsar.common.config.CapabilityTypes
import ai.platon.pulsar.common.config.ImmutableConfig
import ai.platon.pulsar.common.config.Parameterized
import ai.platon.pulsar.common.event.AbstractEventEmitter
import ai.platon.pulsar.common.files.ext.export
import ai.platon.pulsar.common.metrics.AppMetrics
import ai.platon.pulsar.common.persist.ext.options
import ai.platon.pulsar.crawl.fetch.FetchTask
import ai.platon.pulsar.crawl.fetch.driver.WebDriver
import ai.platon.pulsar.crawl.fetch.driver.WebDriverCancellationException
import ai.platon.pulsar.crawl.protocol.ForwardingResponse
import ai.platon.pulsar.crawl.protocol.Response
import ai.platon.pulsar.persist.PageDatum
import ai.platon.pulsar.persist.ProtocolStatus
import ai.platon.pulsar.persist.WebPage
import ai.platon.pulsar.protocol.browser.driver.WebDriverSettings
import ai.platon.pulsar.protocol.browser.emulator.*
import kotlinx.coroutines.delay
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean

abstract class BrowserEmulatorImplBase(
    val driverSettings: WebDriverSettings,
    /**
     * Handle the response
     * */
    val responseHandler: BrowserResponseHandler,
    val immutableConfig: ImmutableConfig
): AbstractEventEmitter<EmulateEvents>(), Parameterized, AutoCloseable {
    private val logger = getLogger(BrowserEmulatorImplBase::class)
    private val tracer = logger.takeIf { it.isTraceEnabled }
    val supportAllCharsets get() = immutableConfig.getBoolean(CapabilityTypes.PARSE_SUPPORT_ALL_CHARSETS, true)
    val charsetPattern = if (supportAllCharsets) SYSTEM_AVAILABLE_CHARSET_PATTERN else DEFAULT_CHARSET_PATTERN
    val closed = AtomicBoolean(false)
    val isActive get() = !closed.get() && AppContext.isActive

    protected val pageSourceByteHistogram by lazy { registry.histogram(this, "hPageSourceBytes") }
    private val registry = AppMetrics.reg
    protected val pageSourceBytes by lazy { registry.meter(this, "pageSourceBytes") }

    val meterNavigates by lazy { registry.meter(this, "navigates") }
    val counterJsEvaluates by lazy { registry.counter(this, "jsEvaluates") }
    val counterJsWaits by lazy { registry.counter(this, "jsWaits") }
    val counterCancels by lazy { registry.counter(this, "cancels") }

    open fun createResponse(task: NavigateTask): Response {
        if (!isActive) {
            return ForwardingResponse.canceled(task.page)
        }

        val pageDatum = task.pageDatum
        val length = task.pageSource.length
        pageSourceByteHistogram.update(length)
        pageSourceBytes.mark(length.toLong())

        pageDatum.pageCategory = responseHandler.pageCategorySniffer(pageDatum)
        pageDatum.protocolStatus = responseHandler.checkErrorPage(task.page, pageDatum.protocolStatus)
        if (!pageDatum.protocolStatus.isSuccess) {
            // The browser shows internal error page, which is no value to store
            task.pageSource = ""
            pageDatum.lastBrowser = task.driver.browserType
            return createResponseWithDatum(task, pageDatum)
        }

        // Check if the page source is integral
        val integrity = responseHandler.htmlIntegrityChecker(task.pageSource, task.pageDatum)
        // Check browse timeout event, transform status to be success if the page source is good
        if (pageDatum.protocolStatus.isTimeout) {
            if (integrity.isOK) {
                // fetch timeout but content is OK
                pageDatum.protocolStatus = ProtocolStatus.STATUS_SUCCESS
            }
            responseHandler.emit(BrowserResponseEvents.browseTimeout)
        }

        pageDatum.headers.put(HttpHeaders.CONTENT_LENGTH, task.pageSource.length.toString())
        if (integrity.isOK) {
            // Update page source, modify charset directive, do the caching stuff
            task.pageSource = responseHandler.normalizePageSource(task.url, task.pageSource).toString()
        } else {
            // The page seems to be broken, retry it
            pageDatum.protocolStatus = responseHandler.createProtocolStatusForBrokenContent(task.fetchTask, integrity)
            logBrokenPage(task.fetchTask, task.pageSource, integrity)
        }

        pageDatum.apply {
            lastBrowser = task.driver.browserType
            htmlIntegrity = integrity
            originalContentLength = task.originalContentLength
            content = task.pageSource.toByteArray(StandardCharsets.UTF_8)
        }

        // Update headers, metadata, do the logging stuff
        return createResponseWithDatum(task, pageDatum)
    }

    open fun createResponseWithDatum(task: NavigateTask, pageDatum: PageDatum): ForwardingResponse {
        val headers = pageDatum.headers

        // The page content's encoding is already converted to UTF-8 by Web driver
        val utf8 = StandardCharsets.UTF_8.name()
        require(utf8 == "UTF-8") { "UTF-8 is expected" }

        headers.put(HttpHeaders.CONTENT_ENCODING, utf8)
        headers.put(HttpHeaders.Q_TRUSTED_CONTENT_ENCODING, utf8)
        headers.put(HttpHeaders.Q_RESPONSE_TIME, System.currentTimeMillis().toString())

        val urls = pageDatum.activeDOMUrls
        if (urls != null) {
            pageDatum.location = urls.location
            if (pageDatum.url != pageDatum.location) {
                // in-browser redirection
                // messageWriter?.debugRedirects(pageDatum.url, urls)
            }
        }

        if (!task.driver.isMockedPageSource) {
            exportIfNecessary(task)
        }

        return ForwardingResponse(task.page, pageDatum)
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {

        }
    }

    @Throws(NavigateTaskCancellationException::class)
    protected fun checkState() {
        if (!isActive) {
            throw NavigateTaskCancellationException("Emulator is closed")
        }
    }

    /**
     * Check the task state.
     * */
    @Throws(NavigateTaskCancellationException::class)
    protected fun checkState(driver: WebDriver) {
        checkState()

        if (driver.isCanceled) {
            // the task is canceled, so the navigation is stopped, the driver is closed, the privacy context is reset
            // and all the running tasks should be redo
            throw WebDriverCancellationException("Web driver is canceled #${driver.id}", driver)
        }
    }

    /**
     * Check the task state.
     * */
    @Throws(NavigateTaskCancellationException::class, WebDriverCancellationException::class)
    protected fun checkState(task: FetchTask, driver: WebDriver) {
        checkState()

        if (driver.isCanceled) {
            // the task is canceled, so the navigation is stopped, the driver is closed, the privacy context is reset
            // and all the running tasks should be redo
            throw WebDriverCancellationException("Web driver is canceled #${driver.id}", driver)
        }

        if (task.isCanceled) {
            // the task is canceled, so the navigation is stopped, the driver is closed, the privacy context is reset
            // and all the running tasks should be redo
            throw NavigateTaskCancellationException("Task #${task.batchTaskId}/${task.batchId} is canceled | ${task.url}")
        }
    }

    protected fun logBeforeNavigate(task: FetchTask, driverSettings: BrowserSettings) {
        if (logger.isTraceEnabled) {
            val settings = driverSettings.interactSettings
            logger.trace(
                "Navigate {}/{}/{} in [t{}]{} | {} | timeouts: {}/{}/{}",
                task.batchTaskId, task.batchSize, task.id,
                Thread.currentThread().id,
                if (task.nRetries <= 1) "" else "(${task.nRetries})",
                task.page.configuredUrl,
                settings.pageLoadTimeout, settings.scriptTimeout, settings.scrollInterval
            )
        }
    }

    /**
     * Export the page if one of the following condition matches:
     * 1. the first 200 pages
     * 2. LoadOptions.test > 0
     * 3. logger level is debug or lower
     * 4. logger level is info and protocol status is failed
     * */
    private fun exportIfNecessary(task: NavigateTask) {
        try {
            exportIfNecessary0(task.pageSource, task.pageDatum.protocolStatus, task.page)
        } catch (e: Exception) {
            logger.warn("Failed to export webpage | {} | \n{}", task.url, e.stringify())
        }
    }

    /**
     * Export the page if one of the following condition matches:
     * 1. the first 200 pages
     * 2. LoadOptions.test > 0
     * 3. logger level is debug or lower
     * 4. logger level is info and protocol status is failed
     * */
    private fun exportIfNecessary0(pageSource: String, status: ProtocolStatus, page: WebPage) {
        if (pageSource.isEmpty()) {
            return
        }

        val id = page.id
        val test = page.options.test
        val shouldExport =
            id < 200 || id % 100 == 0 || test > 0 || logger.isDebugEnabled || (logger.isInfoEnabled && !status.isSuccess)
        if (shouldExport) {
            export0(pageSource, status, page)
        }
    }

    private fun export0(pageSource: String, status: ProtocolStatus, page: WebPage) {
        if (pageSource.isEmpty()) {
            return
        }

        val path = AppFiles.export(status, pageSource, page)

        // Create a symbolic link with an url based, unique, shorter but not readable file name,
        // we can generate and refer to this path at any place
        val link = AppPaths.uniqueSymbolicLinkForUri(page.url)
        try {
            Files.deleteIfExists(link)
            Files.createSymbolicLink(link, path)
        } catch (e: IOException) {
            logger.warn(e.toString())
        }
    }

    private fun logBrokenPage(task: FetchTask, pageSource: String, integrity: HtmlIntegrity) {
        if (!isActive) {
            return
        }

        val proxyEntry = task.proxyEntry
        val domain = task.domain
        val link = AppPaths.uniqueSymbolicLinkForUri(task.url)
        val readableLength = Strings.compactFormat(pageSource.length)

        if (proxyEntry != null) {
            val count = proxyEntry.servedDomains.count(domain)
            logger.warn(
                "{}. Page is {}({}) with {} in {}({}) | file://{}",
                task.page.id,
                integrity.name, readableLength,
                proxyEntry.display, domain, count, link
            )
        } else {
            logger.warn("{}. Page is {}({}) | file://{} | {}",
                task.page.id, integrity.name, readableLength, link, task.url)
        }
    }

    protected suspend fun evaluate(
        interactTask: InteractTask, expressions: Iterable<String>, delayMillis: Long,
        bringToFront: Boolean = false, verbose: Boolean = false
    ) {
        expressions.asSequence()
            .mapNotNull { it.trim().takeIf { it.isNotBlank() } }
            .filterNot { it.startsWith("// ") }
            .filterNot { it.startsWith("# ") }
            .forEachIndexed { i, expression ->
                if (bringToFront && i % 2 == 0) {
                    interactTask.driver.bringToFront()
                }

                evaluate(interactTask, expression, verbose)
                delay(delayMillis)
            }
    }

    protected suspend fun evaluate(
        interactTask: InteractTask, expression: String, verbose: Boolean
    ): Any? {
        logger.takeIf { verbose }?.info("Evaluate expression >>>$expression<<<")
        val value = evaluate(interactTask, expression)
        if (value is String) {
            val s = Strings.stripNonPrintableChar(value)
            logger.takeIf { verbose }?.info("Result >>>$s<<<")
        } else if (value is Int || value is Long) {
            logger.takeIf { verbose }?.info("Result >>>$value<<<")
        }
        return value
    }

    @Throws(WebDriverCancellationException::class)
    protected suspend fun evaluate(interactTask: InteractTask, expression: String, delayMillis: Long = 0): Any? {
        if (!isActive) return null

        counterJsEvaluates.inc()
        checkState(interactTask.navigateTask.fetchTask, interactTask.driver)
        val result = interactTask.driver.evaluate(expression)
        if (delayMillis > 0) {
            delay(delayMillis)
        }
        return result
    }
}
