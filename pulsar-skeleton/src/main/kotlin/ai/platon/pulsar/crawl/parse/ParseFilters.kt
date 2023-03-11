package ai.platon.pulsar.crawl.parse

import ai.platon.pulsar.common.brief
import ai.platon.pulsar.common.config.ImmutableConfig
import ai.platon.pulsar.crawl.parse.html.ParseContext
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.reflect.KClass

/**
 * List of [ParseFilter] plugins.
 *
 * [ParseFilter]s are invoked after the page content be parsed, so we can do jobs
 * with the parse result, such as:
 * 1. extract data from the parsed document
 * 2. persist extracted data to external storage
 */
class ParseFilters(initParseFilters: List<ParseFilter>, val conf: ImmutableConfig): AutoCloseable {
    private val logger = LoggerFactory.getLogger(ParseFilters::class.java)

    private val _parseFilters = Collections.synchronizedList(initParseFilters.toMutableList())
    private val closed = AtomicBoolean()

    val parseFilters: List<ParseFilter> = _parseFilters

    constructor(conf: ImmutableConfig): this(listOf(), conf)

    fun initialize() {
        parseFilters.forEach { it.initialize() }
    }

    fun clear() = _parseFilters.clear()

    fun remove(clazz: KClass<*>) {
        val it = _parseFilters.iterator()
        while (it.hasNext() && it.next()::class == clazz) {
            it.remove()
        }
    }

    fun hasFilter(parseFilter: ParseFilter) = parseFilters.contains(parseFilter)

    fun addFirst(parseFilter: ParseFilter) = _parseFilters.add(0, parseFilter)

    fun addLast(parseFilter: ParseFilter) = _parseFilters.add(parseFilter)

    /**
     * Run all defined filters
     */
    fun filter(parseContext: ParseContext) {
        // loop on each filter
        parseFilters.forEach { filter ->
            if (filter.isRelevant(parseContext).isOK) {
                val result = filter.runCatching { filter(parseContext) }
                    .onFailure { logger.warn("[Unexpected]", it) }.getOrNull()

                if (result != null && result.shouldBreak) {
                    return@filter
                }
            }
        }
    }

    fun report(sb: StringBuilder = StringBuilder()) {
        parseFilters.forEach {
            report(it, 0, sb)
        }
    }

    override fun toString(): String {
        return parseFilters.joinToString { it.javaClass.simpleName }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            parseFilters.forEach { filter ->
                runCatching { filter.close() }.onFailure { t ->
                    logger.warn(t.brief("Failed to close ${filter.javaClass.simpleName}"))
                }
            }
        }
    }

    private fun report(filter: ParseFilter, depth: Int, sb: StringBuilder) {
        val padding = if (depth > 0) "  ".repeat(depth) else ""
        sb.appendLine("${padding}$filter")
        filter.children.forEach {
            report(it, depth + 1, sb)
        }
    }
}
