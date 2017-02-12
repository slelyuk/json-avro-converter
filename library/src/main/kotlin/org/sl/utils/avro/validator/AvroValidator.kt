package org.sl.utils.avro.validator

import com.google.common.reflect.TypeToken
import org.apache.avro.Schema
import org.sl.utils.avro.converter.JsonAvroConverter
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.IOException

class AvroValidator(schema: ByteArray?, content: ByteArray?, mode: ValidationMode) : Validator {

    private val logger = LoggerFactory.getLogger(AvroValidator::class.java)

    private val schema: Schema

    private val content: ByteArray?

    private val mode: ValidationMode

    private val converter: JsonAvroConverter = JsonAvroConverter()

    init {
        object : TypeToken<Map<*, *>>() {

        }

        try {
            this.schema = Schema.Parser().parse(ByteArrayInputStream(schema))
            this.content = content
            this.mode = mode
        } catch (e: IOException) {
            throw ValidatorException("An unexpected error occurred when parsing the schema", e)
        }

    }

    override fun validate(): ValidationResult {
        val result: ByteArray
        when (mode) {
            ValidationMode.AVRO_TO_JSON -> result = convertAvroToJson(content)
            ValidationMode.JSON_TO_AVRO_TO_JSON -> result = convertAvroToJson(convertJsonToAvro(content))
            ValidationMode.JSON_TO_AVRO -> result = convertJsonToAvro(content)
        }
        return ValidationResult.success(String(result))
    }

    private fun convertAvroToJson(avro: ByteArray?): ByteArray {
        try {
            logger.debug("Converting AVRO to JSON")
            val json = converter.convertToJson(avro!!, schema)
            logger.debug("Validation result: success. JSON: \n{}", String(json))
            return json
        } catch (e: RuntimeException) {
            throw ValidatorException("Error occurred when validating the document", e)
        }

    }

    private fun convertJsonToAvro(json: ByteArray?): ByteArray {
        try {
            logger.debug("Converting JSON to AVRO")
            val avro = converter.convertToAvro(json!!, schema)
            logger.debug("Validation result: success. AVRO: \n{}", String(avro))
            return avro
        } catch (e: RuntimeException) {
            throw ValidatorException("Error occurred when validating the document", e)
        }

    }

    class Builder {

        private var input: ByteArray? = null

        private var schema: ByteArray? = null

        private var mode = ValidationMode.JSON_TO_AVRO

        fun withSchema(schema: ByteArray?): Builder {
            this.schema = schema
            return this
        }

        fun withInput(input: ByteArray?): Builder {
            this.input = input
            return this
        }

        fun withMode(mode: ValidationMode): Builder {
            this.mode = mode
            return this
        }

        fun build(): AvroValidator {
            return AvroValidator(schema, input, mode)
        }
    }

    companion object {

        fun builder(): Builder {
            return Builder()
        }
    }
}
