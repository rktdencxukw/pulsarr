package ai.platon.pulsar.common.websocket

data class ScrapeNodeRegisterInfo (
    // 2023年04月21日，必须nullable，否则会报错，原因未知
    var nodeId: String?=null,
    var ipType: String?=null,
    var fetchModeSupport: String?=null
)