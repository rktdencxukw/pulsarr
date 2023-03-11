package ai.platon.pulsar.browser.common

import org.apache.commons.lang3.RandomStringUtils

class ScriptConfuser {
    companion object {
        const val SCRIPT_NAME_PREFIX = "__pulsar_"
        /**
         * The name cipher for all injected scripts.
         * All names in injected scripts must not be detected by javascript,
         * the name mangling technology helps to achieve this purpose.
         * */
        val CIPHER = RandomStringUtils.randomAlphabetic(6)

        /**
         * The default name mangler replaces all `__pulsar_` to a random string
         * */
        val DEFAULT_NAME_MANGLER: (String) -> String = { script ->
            script.replace(SCRIPT_NAME_PREFIX, CIPHER)
        }

        /**
         * The identity name mangler keeps the script unchanged
         * */
        val IDENTITY_NAME_MANGLER: (String) -> String = { script -> script }
    }

    var nameMangler: (String) -> String = DEFAULT_NAME_MANGLER

    fun confuse(script: String): String {
        return nameMangler(script)
    }

    fun reset() {
        nameMangler = DEFAULT_NAME_MANGLER
    }
}
