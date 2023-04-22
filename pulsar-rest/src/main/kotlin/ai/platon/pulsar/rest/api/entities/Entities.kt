package ai.platon.pulsar.rest.api.entities

import ai.platon.pulsar.common.ResourceStatus
import ai.platon.pulsar.persist.ProtocolStatus
import ai.platon.pulsar.persist.metadata.ProtocolStatusCodes
import java.time.Instant

class ScrapeRequest {
    var sql: String
    var authToken: String = ""
    var priority: String = "HIGHER2"
    var asap: Boolean = false
    var reportUrl: String = ""

    constructor() {
        this.sql = ""
        this.authToken = ""
        this.priority = "HIGHER2"
        this.asap = false
        this.reportUrl = ""
    }

    constructor(sql: String) {
        this.sql = sql
        this.authToken = ""
        this.priority = "HIGHER2"
        this.asap = false
        this.reportUrl = ""
    }

    constructor(
        sql: String,
        authToken: String,
        priority: String = "HIGHER2",
        asap: Boolean = false,
        reportUrl: String = ""
    ) {
        this.sql = sql
        this.authToken = authToken
        this.priority = priority
        this.asap = asap
        this.reportUrl = reportUrl
    }

    override fun toString(): String {
        // use a StringBuilder to avoid creating a lot of String objects
        val sb = StringBuilder()
        sb.append("ScrapeRequest(")
        sb.append("sql=").append(sql)
        sb.append(", authToken=").append(authToken)
        sb.append(", priority=").append(priority)
        sb.append(", asap=").append(asap)
        sb.append(", reportUrl=").append(reportUrl)
        sb.append(")")
        return sb.toString()
    }
}

data class ScrapeRequestSubmitResponse(
    var uuid: String? = null,
    var code: Int = 0,
    var errorMessage: String = ""
) {
}

data class ScrapeResponse(
    var uuid: String? = null,
    var statusCode: Int = ResourceStatus.SC_CREATED,
    var pageStatusCode: Int = ProtocolStatusCodes.CREATED,
    var pageContentBytes: Int = 0,
    var isDone: Boolean = false,
    var resultSet: List<Map<String, Any?>>? = null,
) {
    val status: String get() = ResourceStatus.getStatusText(statusCode)
    val pageStatus: String get() = ProtocolStatus.getMinorName(pageStatusCode)
    val createTime: Instant = Instant.now()
    var finishTime: Instant = Instant.EPOCH
}

data class ScrapeStatusRequest(
    val uuid: String,
)

/**
 * W3 resources
 * */
data class W3DocumentRequest(
    var url: String,
    val args: String? = null,
)
