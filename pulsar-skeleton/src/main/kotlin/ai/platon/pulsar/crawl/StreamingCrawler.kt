package ai.platon.pulsar.crawl

import ai.platon.pulsar.PulsarSession
import ai.platon.pulsar.common.*
import ai.platon.pulsar.common.collect.ConcurrentLoadingIterable
import ai.platon.pulsar.common.config.CapabilityTypes.BROWSER_MAX_ACTIVE_TABS
import ai.platon.pulsar.common.config.CapabilityTypes.PRIVACY_CONTEXT_NUMBER
import ai.platon.pulsar.common.message.CompletedPageFormatter
import ai.platon.pulsar.common.options.LoadOptions
import ai.platon.pulsar.common.options.LoadOptionsNormalizer
import ai.platon.pulsar.common.proxy.ProxyException
import ai.platon.pulsar.common.proxy.ProxyInsufficientBalanceException
import ai.platon.pulsar.common.proxy.ProxyPool
import ai.platon.pulsar.common.proxy.ProxyVendorUntrustedException
import ai.platon.pulsar.common.url.UrlAware
import ai.platon.pulsar.context.PulsarContexts
import ai.platon.pulsar.crawl.common.GlobalCache
import ai.platon.pulsar.crawl.common.ListenableHyperlink
import ai.platon.pulsar.crawl.fetch.privacy.PrivacyContext
import ai.platon.pulsar.persist.WebPage
import com.codahale.metrics.Gauge
import kotlinx.coroutines.*
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import oshi.SystemInfo
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFilePermissions
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs

open class StreamingCrawler<T: UrlAware>(
        private val urls: Sequence<T>,
        private val options: LoadOptions = LoadOptions.create(),
        session: PulsarSession = PulsarContexts.createSession(),
        /**
         * A optional global cache which will hold the retry tasks
         * */
        val globalCache: GlobalCache? = null,
        autoClose: Boolean = true
): AbstractCrawler(session, autoClose) {
    companion object {
        private val instanceSequencer = AtomicInteger()
        private val globalRunningInstances = AtomicInteger()

        private val globalRunningTasks = AtomicInteger()
        private val globalTimeouts = AppMetrics.meter(this, "timeouts")
        private val meterGlobalRetries = AppMetrics.meter(this, "retries")
        private val meterGlobalGone = AppMetrics.meter(this, "gone")
        private val meterGlobalTasks = AppMetrics.meter(this, "tasks")
        private val meterGlobalSuccesses = AppMetrics.meter(this, "successes")
        private val meterGlobalUpdateSuccesses = AppMetrics.meter(this, "updateSuccesses")
        private val meterGlobalFinishes = AppMetrics.meter(this, "finishes")

        private val globalLoadingUrls = ConcurrentSkipListSet<String>()
        private val systemInfo = SystemInfo()
        // OSHI cached the value, so it's fast and safe to be called frequently
        private val availableMemory get() = systemInfo.hardware.memory.available
        private val requiredMemory = 500 * 1024 * 1024L // 500 MiB
        private val remainingMemory get() = availableMemory - requiredMemory
        private val isIllegalApplicationState = AtomicBoolean()

        init {
            mapOf(
                    "availableMemory" to Gauge { Strings.readableBytes(availableMemory) },
                    "globalRunningInstances" to Gauge { globalRunningInstances.get() },
                    "globalRunningTasks" to Gauge { globalRunningTasks.get() },
                    "globalTasks" to Gauge { meterGlobalTasks.count },
                    "globalSuccesses" to Gauge { meterGlobalSuccesses.count },
                    "globalUpdateSuccesses" to Gauge { meterGlobalUpdateSuccesses.count },
                    "globalFinishes" to Gauge { meterGlobalFinishes.count },
                    "globalRetries" to Gauge { meterGlobalRetries.count },
                    "globalTimeouts" to Gauge { globalTimeouts.count },
                    "globalGone" to Gauge { meterGlobalGone.count },
                    "globalTasks/s" to Gauge { meterGlobalTasks.meanRate },
                    "globalSuccesses/s" to Gauge { meterGlobalSuccesses.meanRate },
                    "globalFinishes/s" to Gauge { meterGlobalFinishes.meanRate },
                    "globalRetries/s" to Gauge { meterGlobalRetries.meanRate },
                    "globalGone/s" to Gauge { meterGlobalGone.meanRate },
            ).let { AppMetrics.registerAll(this, it) }
        }
    }

    private val log = LoggerFactory.getLogger(StreamingCrawler::class.java)
    private val conf = session.sessionConfig
    private val numPrivacyContexts get() = conf.getInt(PRIVACY_CONTEXT_NUMBER, 2)
    private val numMaxActiveTabs get() = conf.getInt(BROWSER_MAX_ACTIVE_TABS, AppContext.NCPU)
    private val fetchConcurrency get() = numPrivacyContexts * numMaxActiveTabs
    private val idleTimeout = Duration.ofMinutes(20)
    private var lastActiveTime = Instant.now()
    private val idleTime get() = Duration.between(lastActiveTime, Instant.now())
    private val isIdle get() = idleTime > idleTimeout
    @Volatile
    private var proxyInsufficientBalance = 0
    private var quit = false
    override val isActive get() = super.isActive && !quit && !isIllegalApplicationState.get()
    private val taskTimeout = Duration.ofMinutes(10)
    private var flowState = FlowState.CONTINUE

    var jobName: String = "crawler-" + RandomStringUtils.randomAlphanumeric(5)
    var pageCollector: ConcurrentLinkedQueue<WebPage>? = null
    var proxyPool: ProxyPool? = null

    val id = instanceSequencer.incrementAndGet()

    val crawlerEventHandler = ChainedStreamingCrawlerEventHandler()

    private val gauges = mapOf(
            "idleTime" to Gauge { idleTime.readable() }
    )

    init {
        AppMetrics.registerAll(this, "$id.g", gauges)
        generateFinishCommand()
    }

    open fun run() {
        runBlocking {
            supervisorScope {
                run(this)
            }
        }
    }

    open suspend fun run(scope: CoroutineScope) {
        runInScope(scope)
    }

    fun quit() {
        quit = true
    }

    protected suspend fun runInScope(scope: CoroutineScope) {
        log.info("Starting streaming crawler ... | {}", options)

        globalRunningInstances.incrementAndGet()

        val startTime = Instant.now()

        while (isActive) {
            if (!urls.iterator().hasNext()) {
                sleepSeconds(1)
            }

            urls.forEachIndexed { j, url ->
                if (!isActive) {
                    return@runInScope
                }

                if (url.isNil) {
                    return@forEachIndexed
                }

                if (true == globalCache?.fetchingUrls?.contains(url.url)) {
                    return@forEachIndexed
                }

                if (url.url in globalLoadingUrls) {
                    return@forEachIndexed
                }

                globalLoadingUrls.add(url.url)

                val state = try {
                    load(1 + j, url, scope)
                } finally {
                    meterGlobalFinishes.mark()
                    globalLoadingUrls.remove(url.url)
                }

                if (state != FlowState.CONTINUE) {
                    return@runInScope
                }
            }
        }

        globalRunningInstances.decrementAndGet()

        log.info("All done. Total {} tasks are processed in session {} in {}",
                meterGlobalTasks.count, session, DateTimes.elapsedTime(startTime).readable())
    }

    private suspend fun load(j: Int, url: UrlAware, scope: CoroutineScope): FlowState {
        lastActiveTime = Instant.now()
        meterGlobalTasks.mark()

        while (isActive && globalRunningTasks.get() > fetchConcurrency) {
            if (j % 120 == 0) {
                log.info("{}. It takes long time to run {} tasks | {} -> {}",
                        j, globalRunningTasks, lastActiveTime, idleTime.readable())
            }
            delay(1000)
        }

        var k = 0
        while (isActive && remainingMemory < 0) {
            if (k++ % 20 == 0) {
                handleMemoryShortage(k)
            }
            delay(1000)
        }

        /**
         * The vendor proclaimed every ip can be used for more than 5 minutes,
         * If proxy is not enabled, the rate is always 0
         *
         * 5 / 60f = 0.083
         * */
        val contextLeaksRate = PrivacyContext.meterGlobalContextLeaks.fifteenMinuteRate
        k = 0
        while (isActive && contextLeaksRate >= 5 / 60f && ++k < 300) {
            if (k % 60 == 0) {
                log.warn("Context leaks too fast: $contextLeaksRate leaks/seconds")
            }
            delay(1000)
        }

        while (isActive && proxyInsufficientBalance > 0) {
            delay(1000)
            handleProxySufficientBalance()
        }

        if (FileCommand.check("finish-job")) {
            log.info("Find finish-job command, quit streaming crawler ...")
            return FlowState.BREAK
        }

        if (!isActive) {
            return FlowState.BREAK
        }

        val context = Dispatchers.Default + CoroutineName("w")
        // must increase before launch because we have to control the number of running tasks
        globalRunningTasks.incrementAndGet()
        scope.launch(context) {
            load(url)
            globalRunningTasks.decrementAndGet()
        }

        return flowState
    }

    private suspend fun load(url: UrlAware): WebPage? {
        if (url is ListenableHyperlink) {
            url.onFilter(url.url) ?: return null
        }

        var page: WebPage? = null
        try {
            if (!isActive) {
                return null
            }

            page = withTimeout(taskTimeout.toMillis()) { loadWithEventHandlers(url) }
            if (page != null && page.protocolStatus.isSuccess) {
                if (page.isUpdated) {
                    meterGlobalUpdateSuccesses.mark()
                }
                meterGlobalSuccesses.mark()
            }
        } catch (e: TimeoutCancellationException) {
            globalTimeouts.mark()
            log.info("{}. Task timeout ({}) to load page | {}", globalTimeouts.count, taskTimeout, url)
        } catch (e: Throwable) {
            if (e.javaClass.name == "kotlinx.coroutines.JobCancellationException") {
                if (isIllegalApplicationState.compareAndSet(false, true)) {
                    AppContext.tryTerminate()
                    log.warn("Streaming crawler coroutine was cancelled, quit ...", e)
                }
                flowState = FlowState.BREAK
            } else {
                log.warn("Unexpected exception", e)
            }
        }

        if (!isActive) {
            return null
        }

        if (page == null || page.protocolStatus.isRetry) {
            handleRetry(url, page)
        }

        lastActiveTime = Instant.now()
        page?.let { crawlerEventHandler.onAfterLoad(url, it) }

        // if urls is ConcurrentLoadingIterable
        // TODO: the line below can be removed
        (urls.iterator() as? ConcurrentLoadingIterable.LoadingIterator)?.tryLoad()

        return page
    }

    @Throws(Exception::class)
    private suspend fun loadWithEventHandlers(url: UrlAware): WebPage? {
        val actualOptions = LoadOptionsNormalizer.normalize(options, url)
        registerHandlers(url, actualOptions)

        actualOptions.apply {
            // TODO: it seems there is an option merge bug
            parse = true
        }

        val normUrl = session.normalize(url, actualOptions)
        return session.runCatching { loadDeferred(normUrl) }
                .onFailure { flowState = handleException(url, it) }
                .getOrNull()
                ?.also { pageCollector?.add(it) }
    }

    private fun registerHandlers(url: UrlAware, options: LoadOptions) {
        val volatileConfig = conf.toVolatileConfig().apply { name = options.label }
        options.volatileConfig = volatileConfig
        val eventHandler = (url as? ListenableHyperlink)
                ?.let { DefaultCrawlEventHandler.create(url) }
                ?: DefaultCrawlEventHandler()
        eventHandler.onAfterFetch = ChainedWebPageHandler()
                .addFirst { AddRefererAfterFetchHandler(url) }
                .addLast { eventHandler.onAfterFetch }
        volatileConfig.putBean(eventHandler)
    }

    @Throws(Exception::class)
    private fun handleException(url: UrlAware, e: Throwable): FlowState {
        if (flowState == FlowState.BREAK) {
            return flowState
        }

        when (e) {
            is IllegalApplicationContextStateException -> {
                if (isIllegalApplicationState.compareAndSet(false, true)) {
                    AppContext.tryTerminate()
                    log.warn("\n!!!Illegal application context, quit ... | {}", e.message)
                }
                return FlowState.BREAK
            }
            is IllegalStateException -> {
                log.warn("Illegal state", e)
            }
            is ProxyInsufficientBalanceException -> {
                proxyInsufficientBalance++
                log.warn("{}. {}", proxyInsufficientBalance, e.message)
            }
            is ProxyVendorUntrustedException -> {
                log.warn("Proxy is untrusted | {}", e.message)
                return FlowState.BREAK
            }
            is ProxyException -> {
                log.warn("Unexpected proxy exception | {}", Strings.simplifyException(e))
            }
            is TimeoutCancellationException -> {
                log.warn("Timeout cancellation: {} | {}", Strings.simplifyException(e), url)
            }
            is CancellationException -> {
                // Comes after TimeoutCancellationException
                if (isIllegalApplicationState.compareAndSet(false, true)) {
                    AppContext.tryTerminate()
                    log.warn("Streaming crawler job was canceled, quit ...", e)
                }
                return FlowState.BREAK
            }
            else -> throw e
        }

        return FlowState.CONTINUE
    }

    private fun handleRetry(url: UrlAware, page: WebPage?) {
        val gCache = globalCache
        if (gCache != null && !gCache.isFetching(url)) {
            val cache = gCache.fetchCacheManager.normalCache.nReentrantQueue
            if (cache.add(url)) {
                meterGlobalRetries.mark()
                if (page != null) {
                    val retry = 1 + page.fetchRetries
                    log.info("{}", CompletedPageFormatter(page, prefix = "Retrying ${retry}th"))
                }
            } else {
                meterGlobalGone.mark()
                if (page != null) {
                    log.info("{}", CompletedPageFormatter(page, prefix = "Gone"))
                } else {
                    log.info("Page is gone | {}", url)
                }
            }
        }
    }

    private fun handleMemoryShortage(j: Int) {
        log.info("$j.\tnumRunning: {}, availableMemory: {}, requiredMemory: {}, shortage: {}",
                globalRunningTasks,
                Strings.readableBytes(availableMemory),
                Strings.readableBytes(requiredMemory),
                Strings.readableBytes(abs(remainingMemory))
        )
        session.context.clearCaches()
        System.gc()
    }

    private fun handleProxySufficientBalance() {
        if (++proxyInsufficientBalance % 180 == 0) {
            log.warn("Proxy account insufficient balance, check it again ...")
            val p = proxyPool
            if (p != null) {
                p.runCatching { take() }.onFailure {
                    if (it !is ProxyInsufficientBalanceException) {
                        proxyInsufficientBalance = 0
                    }
                }.onSuccess { proxyInsufficientBalance = 0 }
            } else {
                proxyInsufficientBalance = 0
            }
        }
    }

    private fun generateFinishCommand() {
        val finishScriptPath = AppPaths.SCRIPT_DIR.resolve("finish-crawler.sh")
        val cmd = "#bin\necho finish-job $jobName >> " + AppPaths.PATH_LOCAL_COMMAND
        try {
            Files.write(finishScriptPath, cmd.toByteArray(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)
            Files.setPosixFilePermissions(finishScriptPath, PosixFilePermissions.fromString("rwxrw-r--"))
        } catch (e: IOException) {
            log.error(e.toString())
        }
    }
}
