package org.sl.utils.avro.converter

import com.google.common.reflect.TypeToken
import org.apache.avro.AvroRuntimeException
import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecordBuilder
import org.codehaus.jackson.map.ObjectMapper
import java.io.IOException
import java.util.*
import java.util.Optional.ofNullable
import java.util.stream.Collectors.toMap

class JsonGenericRecordReader @JvmOverloads constructor(private val mapper: ObjectMapper = ObjectMapper()) {

    fun read(data: ByteArray, schema: Schema): GenericData.Record {
        try {
            return read(mapper.readValue(data, genericType<Map<*, *>>()) as Map<*, *>, schema)
        } catch (ex: IOException) {
            throw AvroConversionException("Failed to parse json to map format.", ex)
        }

    }

    fun read(json: Map<*, *>, schema: Schema): GenericData.Record {
        val path = ArrayDeque<String>()
        try {
            return readRecord(json, schema, path)
        } catch (ex: AvroRuntimeException) {
            throw AvroConversionException("Failed to convert JSON to Avro", ex)
        }

    }

    private fun readRecord(json: Map<*, *>, schema: Schema, path: Deque<String>): GenericData.Record {
        val record = GenericRecordBuilder(schema)
        json.mapValues { entry ->
            ofNullable(schema.getField(entry.key as String))
                    .ifPresent { field -> record.set(field, read(field, field.schema(), entry.value, path, false)) }
        }
        return record.build()
    }

    private fun read(field: Schema.Field, schema: Schema, value: Any?, path: Deque<String>, silently: Boolean): Any? {
        val pushed = field.name() != path.peek()
        if (pushed) {
            path.push(field.name())
        }
        val result: Any?
        when (schema.type) {
            Schema.Type.RECORD -> result = onValidType(value, genericType<Map<*, *>>(), path, silently, { map -> readRecord(map, schema, path) })
            Schema.Type.ARRAY -> result = onValidType(value, genericType<List<*>>(), path, silently, { list -> readArray(field, schema, list, path) })
            Schema.Type.MAP -> result = onValidType(value, genericType<Map<*, *>>(), path, silently, { map -> readMap(field, schema, map, path) })
            Schema.Type.UNION -> result = readUnion(field, schema, value, path)
            Schema.Type.INT -> result = onValidNumber(value, path, silently, Number::toInt)
            Schema.Type.LONG -> result = onValidNumber(value, path, silently, Number::toLong)
            Schema.Type.FLOAT -> result = onValidNumber(value, path, silently, Number::toFloat)
            Schema.Type.DOUBLE -> result = onValidNumber(value, path, silently, Number::toDouble)
            Schema.Type.BOOLEAN -> result = onValidType(value, Boolean::class.javaObjectType, path, silently, { bool -> bool })
            Schema.Type.ENUM -> result = onValidType(value, String::class.javaObjectType, path, silently, { string -> ensureEnum(schema, string, path) })
            Schema.Type.STRING -> result = onValidType(value, String::class.javaObjectType, path, silently, { string -> string })
            Schema.Type.NULL -> result = if (value == null) value else INCOMPATIBLE
            else -> throw AvroTypeException("Unsupported type: " + field.schema().type)
        }

        if (pushed) {
            path.pop()
        }
        return result
    }

    private fun readArray(field: Schema.Field, schema: Schema, items: List<*>, path: Deque<String>): List<Any?> {
        return items.map({ item -> read(field, schema.elementType, item, path, false) })
    }

    private fun readMap(field: Schema.Field, schema: Schema, map: Map<*, *>, path: Deque<String>): Map<*, *> {
        return map.mapValues { entry -> read(field, schema.valueType, entry.value, path, false) }
    }

    private fun readUnion(field: Schema.Field, schema: Schema, value: Any?, path: Deque<String>): Any? {
        val types = schema.types
        for (type in types) {
            try {
                val nestedValue = read(field, type, value, path, true)
                if (nestedValue === INCOMPATIBLE) {
                    continue
                } else {
                    return nestedValue
                }
            } catch (e: AvroRuntimeException) {
                // thrown only for union of more complex types like records
                continue
            }

        }
        throw TypeExceptions.unionException(
                field.name(),
                types.map({ it.type }).joinToString(", "),
                path)
    }

    private fun ensureEnum(schema: Schema, value: Any, path: Deque<String>): Any {
        val symbols = schema.enumSymbols
        if (symbols.contains(value)) {
            return value
        }
        throw TypeExceptions.enumException(path, symbols.map(String::toString).joinToString(", "))
    }

    @Throws(AvroTypeException::class)
    fun <T: Any?> onValidType(value: Any?, type: Class<in T>, path: Deque<String>, silently: Boolean, function: (T) -> Any): Any {

        if (type.isAssignableFrom(value!!.javaClass)) {
            @Suppress("UNCHECKED_CAST")
            return function.invoke(value as T)
        } else {
            if (silently) {
                return INCOMPATIBLE
            } else {
                throw TypeExceptions.typeException(path, type.typeName)
            }
        }
    }

    fun onValidNumber(value: Any?, path: Deque<String>, silently: Boolean, function: (Number) -> Number): Any {
        return onValidType(value, Number::class.java, path, silently, function)
    }

    companion object {
        private val INCOMPATIBLE = Any()
    }

    inline fun <reified T> genericType() = object : TypeToken<T>() {}.rawType!!

}

