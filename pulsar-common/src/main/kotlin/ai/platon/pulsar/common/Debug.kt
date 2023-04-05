package ai.platon.pulsar.common

class Debug {
    companion object {
        fun getLineInfo(): String {
            val ste = Throwable().stackTrace[1]
            return ste.fileName + ":" + ste.lineNumber
        }
    }
}