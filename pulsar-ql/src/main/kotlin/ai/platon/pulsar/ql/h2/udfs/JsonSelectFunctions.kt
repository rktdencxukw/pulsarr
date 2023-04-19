package ai.platon.pulsar.ql.h2.udfs

import ai.platon.pulsar.ql.annotation.UDFGroup
import ai.platon.pulsar.ql.annotation.UDFunction
import com.jayway.jsonpath.DocumentContext
import org.h2.value.ValueArray
import org.h2.value.ValueString

@Suppress("unused")
@UDFGroup(namespace = "JSON")
object JsonSelectFunctions {

    @UDFunction(description = "Select all elements from a JSON by the given css query and return the the element texts")
    @JvmStatic
    fun allTexts(json: DocumentContext, jsonPathQuery: String): ValueArray {
        var l = json.read<List<Object>>(jsonPathQuery);
        val arr = l.map { ValueString.get(it.toString()) }.toTypedArray()
        return ValueArray.get(arr)
    }
}
