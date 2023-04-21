package ai.platon.pulsar.common.websocket

import java.io.Serializable

open class ExoticResponse<T: Serializable> {
    var code: Long = 0
    var message: String = "ok"
    var data: T? = null


    constructor() {

    }
        constructor(code: Long, message: String, data: T?) {
        this.code = code
        this.message = message
        this.data = data
    }

    constructor(code: Long, message: String) {
        this.code = code
        this.message = message
    }
    constructor(data: T?) {
        this.data = data
    }


    companion object {
        fun <T: Serializable> ok(): ExoticResponse<T> {
            return ExoticResponse()
        }

        fun <T: Serializable> okWithData(data: T): ExoticResponse<T> {
            return ExoticResponse(data = data)
        }

        fun <T: Serializable> okWithData(message: String, data: T): ExoticResponse<T> {
            return ExoticResponse(0, message, data)
        }

        fun <T: Serializable> error(): ExoticResponse<T> {
            return ExoticResponse(1, "error")
        }

        fun <T: Serializable> error(message: String): ExoticResponse<T> {
            return ExoticResponse(1, message)
        }

        fun <T: Serializable> errorWithDatta(data: T): ExoticResponse<T> {
            return ExoticResponse(1, "error", data = data)
        }

        fun <T: Serializable> errorWithData(message: String, data: T): ExoticResponse<T> {
            return ExoticResponse(1, message, data)
        }
    }
}

class Command<O>(val action: String, val reqId: Long) : Serializable {
    var args: List<O>? = null
}

class CommandResponse<O>(val reqId: Long) : Serializable {
    var code: Int = 0
    var message: String = "ok"
    var data: O? = null
}