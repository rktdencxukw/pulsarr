package ai.platon.pulsar.rest.api.common

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.h2.jdbc.JdbcArray
import java.io.IOException

open class JdbcArraySerializer() : StdSerializer<JdbcArray>(JdbcArray::class.java) {

    @Throws(IOException::class)
    override fun serialize(
        value: JdbcArray, gen: JsonGenerator,
        serializers: SerializerProvider
    ) {
        val valueArr = value.array as Array<*>
        gen.writeStartArray(valueArr, valueArr.size)
        for (v in value.array as Array<*>) {
            gen.writeObject(v)
        }
        gen.writeEndArray()
    }
}