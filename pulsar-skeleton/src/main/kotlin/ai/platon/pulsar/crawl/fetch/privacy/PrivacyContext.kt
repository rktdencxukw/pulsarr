package ai.platon.pulsar.crawl.fetch.privacy

import ai.platon.pulsar.common.AppPaths
import ai.platon.pulsar.common.HtmlIntegrity
import ai.platon.pulsar.common.config.AppConstants.FETCH_TASK_TIMEOUT_DEFAULT
import ai.platon.pulsar.common.config.CapabilityTypes.*
import ai.platon.pulsar.common.config.ImmutableConfig
import ai.platon.pulsar.common.metrics.AppMetrics
import ai.platon.pulsar.common.proxy.ProxyException
import ai.platon.pulsar.common.proxy.ProxyRetiredException
import ai.platon.pulsar.common.readable
import ai.platon.pulsar.crawl.fetch.FetchResult
import ai.platon.pulsar.crawl.fetch.FetchTask
import ai.platon.pulsar.crawl.fetch.driver.BrowserErrorPageException
import ai.platon.pulsar.crawl.fetch.driver.WebDriver
import ai.platon.pulsar.persist.RetryScope
import com.google.common.annotations.Beta
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * A privacy context is a standalone agent to the target website, it will be closed once it is leaked.
 *
 * One of the biggest difficulties in web scraping tasks is the bot stealth.
 *
 * For web scraping tasks, the website should have no idea whether a visit is
 * from a human being or a bot. Once a page visit is suspected by the website,
 * which we call a privacy leak, the privacy context has to be dropped,
 * and Pulsar will visit the page in another privacy context.
 * */
abstract class PrivacyContext(
    val id: PrivacyAgent,
    val conf: ImmutableConfig
) : Comparable<PrivacyContext>, AutoCloseable {
    companion object {
        private val instanceSequencer = AtomicInteger()
        val IDENT_PREFIX = "cx."
        val DEFAULT_DIR = AppPaths.CONTEXT_TMP_DIR.resolve("default")
        val PROTOTYPE_CONTEXT_DIR = AppPaths.CHROME_DATA_DIR_PROTOTYPE.parent
        val PROTOTYPE_DATA_DIR = AppPaths.CHROME_DATA_DIR_PROTOTYPE
        val PRIVACY_CONTEXT_IDLE_TIMEOUT_DEFAULT = Duration.ofMinutes(30)

        val globalMetrics by lazy { PrivacyContextMetrics() }
    }

    private val logger = LoggerFactory.getLogger(PrivacyContext::class.java)
    val sequence = instanceSequencer.incrementAndGet()
    val display get() = id.display

    protected val numRunningTasks = AtomicInteger()
    val minimumThroughput = conf.getFloat(PRIVACY_CONTEXT_MIN_THROUGHPUT, 0.3f)
    val maximumWarnings = conf.getInt(PRIVACY_MAX_WARNINGS, 8)
    val minorWarningFactor = conf.getInt(PRIVACY_MINOR_WARNING_FACTOR, 5)
    val privacyLeakWarnings = AtomicInteger()
    val privacyLeakMinorWarnings = AtomicInteger()

    private val registry = AppMetrics.defaultMetricRegistry
    private val sms = AppMetrics.SHADOW_METRIC_SYMBOL
    val meterTasks = registry.meter(this, "$sequence$sms", "tasks")
    val meterSuccesses = registry.meter(this, "$sequence$sms", "successes")
    val meterFinishes = registry.meter(this, "$sequence$sms", "finishes")
    val meterSmallPages = registry.meter(this, "$sequence$sms", "smallPages")
    val smallPageRate get() = 1.0 * meterSmallPages.count / meterTasks.count.coerceAtLeast(1)
    val successRate = meterSuccesses.count.toFloat() / meterTasks.count
    /**
     * The rate of failures. Failure rate is meaningless when there are few tasks.
     * */
    val failureRate get() = 1 - successRate
    val failureRateThreshold = conf.getFloat(PRIVACY_CONTEXT_FAILURE_RATE_THRESHOLD, 0.6f)
    /**
     * Check if failure rate is too high.
     * High failure rate make sense only when there are many tasks.
     * */
    val isHighFailureRate get() = meterTasks.count > 100 && failureRate > failureRateThreshold

    val startTime = Instant.now()
    var lastActiveTime = Instant.now()
    val elapsedTime get() = Duration.between(startTime, Instant.now())
    private val fetchTaskTimeout
        get() = conf.getDuration(FETCH_TASK_TIMEOUT, FETCH_TASK_TIMEOUT_DEFAULT)
    private val privacyContextIdleTimeout
        get() = conf.getDuration(PRIVACY_CONTEXT_IDLE_TIMEOUT, PRIVACY_CONTEXT_IDLE_TIMEOUT_DEFAULT)
    private val idleTimeout: Duration get() = privacyContextIdleTimeout.coerceAtLeast(fetchTaskTimeout)

    val idelTime get() = Duration.between(lastActiveTime, Instant.now())
    open val isIdle get() = idelTime > idleTimeout

//    val historyUrls = PassiveExpiringMap<String, String>()

    protected val closed = AtomicBoolean()
    /**
     * The privacy context works fine and the fetch speed is qualified.
     * */
    open val isGood get() = meterSuccesses.meanRate >= minimumThroughput
    /**
     * The privacy has been leaked since there are too many warnings about privacy leakage.
     * */
    open val isLeaked get() = privacyLeakWarnings.get() >= maximumWarnings
    /**
     * The privacy context works fine and the fetch speed is qualified.
     * */
    open val isRetired get() = false
    /**
     * Check if the privacy context is active.
     * An active privacy context can be used to serve tasks, and an inactive one should be closed.
     * */
    open val isActive get() = !isLeaked && !isRetired && !isClosed
    /**
     * Check if the privacy context is closed
     * */
    open val isClosed get() = closed.get()
    /**
     * A ready privacy context is ready to serve tasks.
     *
     * A ready privacy context has to meet the following requirements:
     * 1. not closed
     * 2. not leaked
     * 3. [requirement removed] not idle
     * 4. if there is a proxy, the proxy has to be ready
     * 5. the associated driver pool promises to provide an available driver, ether one of the following:
     *    1. it has slots to create new drivers
     *    2. it has standby drivers
     *
     * Note: this flag does not guarantee consistency, and can change immediately after it's read
     * */
    open val isReady get() = hasWebDriverPromise() && isActive

    /**
     * Check if the privacy context is running at full load
     * */
    open val isFullCapacity = false

    /**
     * Check if the privacy context is running under loaded
     * */
    open val isUnderLoaded get() = !isFullCapacity

    /**
     * Get the readable privacy context state.
     * */
    open val readableState: String get() {
        return listOf(
            "closed" to isClosed, "leaked" to isLeaked, "active" to isActive,
            "highFailure" to isHighFailureRate, "idle" to isIdle, "good" to isGood,
            "ready" to isReady
        ).filter { it.second }.joinToString(" ") { it.first }
    }

    init {
        globalMetrics.contexts.mark()
    }

    /**
     * The promised worker count.
     *
     * The implementation has to tell the caller how many workers it can provide.
     * The number of workers can change immediately after reading, so the caller only has promises
     * but no guarantees.
     *
     * @return the number of workers promised.
     * */
    abstract fun promisedWebDriverCount(): Int

    /**
     * Check if the privacy context promises at least one worker to provide.
     * */
    fun hasWebDriverPromise() = promisedWebDriverCount() > 0

    @Beta
    abstract fun subscribeWebDriver(): WebDriver?

    /**
     * Mark a success task.
     * */
    fun markSuccess() {
        privacyLeakWarnings.takeIf { it.get() > 0 }?.decrementAndGet()
        meterSuccesses.mark()
        globalMetrics.successes.mark()
    }

    /**
     * Mark a warning.
     * */
    fun markWarning() {
        privacyLeakWarnings.incrementAndGet()
        globalMetrics.leakWarnings.mark()
    }

    /**
     * Mark n warnings.
     * */
    fun markWarning(n: Int) {
        privacyLeakWarnings.addAndGet(n)
        globalMetrics.leakWarnings.mark(n.toLong())
    }

    /**
     * Mark a minor warnings.
     * */
    fun markMinorWarning() {
        privacyLeakMinorWarnings.incrementAndGet()
        globalMetrics.minorLeakWarnings.mark()
        if (privacyLeakMinorWarnings.get() > minorWarningFactor) {
            privacyLeakMinorWarnings.set(0)
            markWarning()
        }
    }

    /**
     * Mark the privacy context as leaked. A leaked privacy context should not serve anymore, 
     * and will be closed soon.
     * */
    fun markLeaked() = privacyLeakWarnings.addAndGet(maximumWarnings)

    /**
     * Run a task in the privacy context and record the status.
     *
     * @param task the fetch task
     * @param fetchFun the fetch function
     * @return the fetch result
     * */
    @Throws(ProxyException::class)
    open suspend fun run(task: FetchTask, fetchFun: suspend (FetchTask, WebDriver) -> FetchResult): FetchResult {
        beforeRun(task)
        val result = doRun(task, fetchFun)
        afterRun(result)
        return result
    }

    /**
     * Run a task in the privacy context.
     *
     * @param task the fetch task
     * @param fetchFun the fetch function
     * @return the fetch result
     * */
    @Throws(ProxyException::class)
    abstract suspend fun doRun(task: FetchTask, fetchFun: suspend (FetchTask, WebDriver) -> FetchResult): FetchResult

    fun takeSnapshot(): String {
        return "$readableState driver: ${promisedWebDriverCount()}"
    }

    /**
     * Do the maintaining jobs.
     * */
    abstract fun maintain()

    override fun compareTo(other: PrivacyContext) = id.compareTo(other.id)

    override fun equals(other: Any?) = other is PrivacyContext && other.id == id

    override fun hashCode() = id.hashCode()

    protected fun beforeRun(task: FetchTask) {
        lastActiveTime = Instant.now()
        meterTasks.mark()
        globalMetrics.tasks.mark()

        numRunningTasks.incrementAndGet()
    }

    protected fun afterRun(result: FetchResult) {
        numRunningTasks.decrementAndGet()
//        historyUrls.add(result.task.url)

        lastActiveTime = Instant.now()
        meterFinishes.mark()
        globalMetrics.finishes.mark()

        val status = result.status
        when {
            status.isRetry(RetryScope.PRIVACY, ProxyRetiredException::class.java) -> markLeaked()
            status.isRetry(RetryScope.PRIVACY, HtmlIntegrity.FORBIDDEN) -> markLeaked()
            status.isRetry(RetryScope.PRIVACY, HtmlIntegrity.ROBOT_CHECK) -> markWarning()
            status.isRetry(RetryScope.PRIVACY, HtmlIntegrity.ROBOT_CHECK_2) -> markWarning(2)
            status.isRetry(RetryScope.PRIVACY, HtmlIntegrity.ROBOT_CHECK_3) -> markWarning(3)
            status.isRetry(RetryScope.PRIVACY, BrowserErrorPageException::class.java) -> markWarning(3)
            status.isRetry(RetryScope.PRIVACY) -> markWarning()
            status.isRetry(RetryScope.CRAWL) -> markMinorWarning()
            status.isSuccess -> markSuccess()
        }

        if (result.isSmall) {
            meterSmallPages.mark()
            globalMetrics.smallPages.mark()
        }

        if (isLeaked) {
            globalMetrics.contextLeaks.mark()
        }
    }

    open fun getReport(): String {
        return String.format("Privacy context #%s has lived for %s", sequence, elapsedTime.readable())
    }

    open fun report() {
        logger.info("Privacy context #{} has lived for {}", sequence, elapsedTime.readable())
    }
}
