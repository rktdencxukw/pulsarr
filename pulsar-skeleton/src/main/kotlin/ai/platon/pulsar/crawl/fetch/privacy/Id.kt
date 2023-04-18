package ai.platon.pulsar.crawl.fetch.privacy

import ai.platon.pulsar.common.AppPaths
import ai.platon.pulsar.common.browser.BrowserType
import ai.platon.pulsar.common.browser.Fingerprint
import ai.platon.pulsar.common.config.CapabilityTypes
import ai.platon.pulsar.common.config.ImmutableConfig
import ai.platon.pulsar.common.readableClassName
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

data class PrivacyAgentId(
    val contextDir: Path,
    val browserType: BrowserType
): Comparable<PrivacyAgentId> {
    override fun compareTo(other: PrivacyAgentId): Int {
        return contextDir.compareTo(other.contextDir)
    }

    val isDefault get() = this == DEFAULT
    val isPrototype get() = this == PROTOTYPE

    companion object {
        val DEFAULT = PrivacyAgentId(PrivacyContext.DEFAULT_DIR, BrowserType.PULSAR_CHROME)
        val PROTOTYPE = PrivacyAgentId(PrivacyContext.PROTOTYPE_CONTEXT_DIR, BrowserType.PULSAR_CHROME)
    }
}

/**
 * The privacy agent defines a unique agent to visit websites.
 *
 * Page visits through different privacy agents should not be detected
 * as the same person, even if the visits are from the same host.
 * */
data class PrivacyAgent(
    val contextDir: Path,
    var fingerprint: Fingerprint
): Comparable<PrivacyAgent> {

    val id = PrivacyAgentId(contextDir, fingerprint.browserType)
    val ident = contextDir.last().toString()
    val display = ident.substringAfter(PrivacyContext.IDENT_PREFIX)
    val browserType get() = fingerprint.browserType

    constructor(contextDir: Path, browserType: BrowserType): this(contextDir, Fingerprint(browserType))

    /**
     * The PrivacyAgent equality.
     * Note: do not use the default equality function
     * */
    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }

        return other is PrivacyAgent
                && other.contextDir == contextDir
                && other.browserType.name == browserType.name && other.fingerprint.proxyServer == other.fingerprint.proxyServer
    }

    override fun hashCode(): Int {
        return 31 * contextDir.hashCode() + browserType.name.hashCode() + fingerprint.proxyServer.hashCode()
    }

    override fun compareTo(other: PrivacyAgent): Int {
        val r = contextDir.compareTo(other.contextDir)
        if (r != 0) {
            return r
        }
//        return fingerprint.compareTo(other.fingerprint)
        val r2 = browserType.name.compareTo(other.browserType.name)
        if (r2 != 0) {
            return r2
        }

        return if (fingerprint.proxyServer == null) {
            if (other.fingerprint.proxyServer == null) {
                0
            } else {
                -1
            }
        } else if (other.fingerprint.proxyServer == null) {
            1
        } else {
            fingerprint.proxyServer!!.compareTo(other.fingerprint.proxyServer!!)
        }
    }

//    override fun toString() = /** AUTO GENERATED **/

    companion object {
        val DEFAULT = PrivacyAgent(PrivacyContext.DEFAULT_DIR, BrowserType.PULSAR_CHROME)
        val PROTOTYPE = PrivacyAgent(PrivacyContext.PROTOTYPE_CONTEXT_DIR, BrowserType.PULSAR_CHROME)
    }
}

@Deprecated("Inappropriate name", ReplaceWith("PrivacyAgentId"))
typealias PrivacyContextId = PrivacyAgent

/**
 * The unique browser id.
 *
 * Every browser instance have a unique fingerprint and a context directory.
 * */
data class BrowserId constructor(
    val contextDir: Path,
    val fingerprint: Fingerprint,
): Comparable<BrowserId> {

    val browserType: BrowserType get() = fingerprint.browserType
    val proxyServer: String? get() = fingerprint.proxyServer

    val userDataDir: Path get() = when {
        contextDir == PrivacyContext.PROTOTYPE_CONTEXT_DIR -> PrivacyContext.PROTOTYPE_DATA_DIR
        else -> contextDir.resolve(browserType.name.lowercase())
    }
    val ident get() = contextDir.last().toString() + browserType.ordinal
    val display get() = ident.substringAfter(PrivacyContext.IDENT_PREFIX)

    constructor(contextDir: Path, browserType: BrowserType): this(contextDir, Fingerprint(browserType))

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }

        return other is BrowserId
                && other.contextDir == contextDir
                && other.browserType.name == browserType.name
    }

    override fun hashCode(): Int {
        return 31 * contextDir.hashCode() + browserType.name.hashCode()
    }

    override fun compareTo(other: BrowserId): Int {
        val r = contextDir.compareTo(other.contextDir)
        if (r != 0) {
            return r
        }
        return browserType.name.compareTo(other.browserType.name)
    }

    override fun toString(): String {
        return "{$fingerprint | $contextDir}"
    }

    companion object {
        val DEFAULT = BrowserId(AppPaths.BROWSER_TMP_DIR, Fingerprint(BrowserType.PULSAR_CHROME))
        val PROTOTYPE = BrowserId(PrivacyContext.PROTOTYPE_CONTEXT_DIR, Fingerprint(BrowserType.PULSAR_CHROME))
    }
}

@Deprecated("Inappropriate name", ReplaceWith("BrowserId"))
typealias BrowserInstanceId = BrowserId

interface PrivacyContextIdGenerator {
    operator fun invoke(fingerprint: Fingerprint): PrivacyContextId
}

class DefaultPrivacyContextIdGenerator: PrivacyContextIdGenerator {
    companion object {
        private val sequencer = AtomicInteger()
        private val nextContextDir
            get() = PrivacyContext.DEFAULT_DIR.resolve(sequencer.incrementAndGet().toString())
    }

    override fun invoke(fingerprint: Fingerprint): PrivacyContextId = PrivacyContextId(nextContextDir, fingerprint)
}

class PrototypePrivacyContextIdGenerator: PrivacyContextIdGenerator {
    override fun invoke(fingerprint: Fingerprint) = PrivacyContextId.PROTOTYPE
}

class SequentialPrivacyContextIdGenerator: PrivacyContextIdGenerator {
    override fun invoke(fingerprint: Fingerprint): PrivacyContextId =
        PrivacyContextId(nextContextDir(), fingerprint)

    @Synchronized
    private fun nextContextDir(): Path {
        sequencer.incrementAndGet()
        val impreciseNumInstances = 1 + Files.list(ROOT_DIR).filter { Files.isDirectory(it) }.count()
        val rand = RandomStringUtils.randomAlphanumeric(5)
        return ROOT_DIR.resolve("${PrivacyContext.IDENT_PREFIX}${sequencer}$rand$impreciseNumInstances")
    }

    companion object {
        /** The root directory of privacy contexts, every context have it's own directory in this fold */
        private val ROOT_DIR = AppPaths.CONTEXT_TMP_DIR
        private val sequencer = AtomicInteger()
    }
}

class PrivacyContextIdGeneratorFactory(val conf: ImmutableConfig) {
    private val logger = LoggerFactory.getLogger(PrivacyContextIdGeneratorFactory::class.java)
    val generator: PrivacyContextIdGenerator by lazy { createIfAbsent(conf) }

    private fun createIfAbsent(conf: ImmutableConfig): PrivacyContextIdGenerator {
        val defaultClazz = DefaultPrivacyContextIdGenerator::class.java
        val clazz = try {
            conf.getClass(CapabilityTypes.PRIVACY_CONTEXT_ID_GENERATOR_CLASS, defaultClazz)
        } catch (e: Exception) {
            logger.warn("Configured proxy loader {}({}) is not found, use default ({})",
                CapabilityTypes.PRIVACY_CONTEXT_ID_GENERATOR_CLASS,
                conf.get(CapabilityTypes.PRIVACY_CONTEXT_ID_GENERATOR_CLASS),
                defaultClazz.simpleName)
            defaultClazz
        }

        logger.info("Using id generator {}", readableClassName(clazz))

        return clazz.constructors.first { it.parameters.isEmpty() }.newInstance() as PrivacyContextIdGenerator
    }
}
