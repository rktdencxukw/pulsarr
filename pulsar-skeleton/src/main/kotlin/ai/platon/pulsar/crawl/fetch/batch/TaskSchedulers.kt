package ai.platon.pulsar.crawl.fetch.batch

import ai.platon.pulsar.common.DateTimes
import ai.platon.pulsar.common.config.ImmutableConfig
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.util.*

class TaskSchedulers(conf: ImmutableConfig) : AutoCloseable {
    val name: String = this.javaClass.simpleName + "-" + DateTimes.now("d.Hms")
    private val fetchSchedulers = Maps.newTreeMap<Int, TaskScheduler>()
    private val fetchSchedulerIds = Lists.newLinkedList<Int>()

    @get:Synchronized
    val first: TaskScheduler
        get() = fetchSchedulers.values.iterator().next()

    constructor(taskSchedulers: List<TaskScheduler>, conf: ImmutableConfig) : this(conf) {
        taskSchedulers.forEach { t -> put(t.id, t) }
    }

    @Synchronized
    fun put(id: Int, taskScheduler: TaskScheduler) {
        fetchSchedulers[id] = taskScheduler
        fetchSchedulerIds.add(id)

        LOG.info("Add task scheduler #$id")
        LOG.info("status: " + __toString())
    }

    @Synchronized
    fun schedulerIds(): List<Int> {
        return ArrayList(fetchSchedulerIds)
    }

    @Synchronized
    operator fun get(id: Int): TaskScheduler? {
        return fetchSchedulers[id]
    }

    @Synchronized
    fun peek(): TaskScheduler? {
        val id = fetchSchedulerIds.peek() ?: return null
        return fetchSchedulers[id]
    }

    @Synchronized
    fun remove(id: Int) {
        fetchSchedulerIds.remove(id)
        fetchSchedulers.remove(id)

        LOG.info("Remove FetchScheduler #$id from pool #$name")
        LOG.info("status: " + __toString())
    }

    @Synchronized
    fun remove(taskScheduler: TaskScheduler?) {
        if (taskScheduler == null) {
            return
        }
        remove(taskScheduler.id)
    }

    @Synchronized
    fun clear() {
        fetchSchedulerIds.clear()
        fetchSchedulers.clear()
    }

    @Synchronized
    override fun toString(): String {
        return __toString()
    }

    private fun __toString(): String {
        val sb = StringBuilder()
        sb.append("Job IDs : ")
                .append(StringUtils.join(fetchSchedulerIds, ", "))
                .append("\tQueue Size : ")
                .append(fetchSchedulers.size)

        return sb.toString()
    }

    @Throws(Exception::class)
    override fun close() {
        LOG.info("[Destruction] Closing TaskSchedulers")

        fetchSchedulerIds.clear()
        fetchSchedulers.clear()
    }

    companion object {
        val LOG = LoggerFactory.getLogger(TaskSchedulers::class.java)
    }
}
