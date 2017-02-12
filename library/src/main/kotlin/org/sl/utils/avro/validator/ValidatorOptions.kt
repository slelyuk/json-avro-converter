package org.sl.utils.avro.validator

class ValidatorOptions(
        /**
         * Schema for validation.
         */
        var schema: ByteArray? = null,
        /**
         * Input for validation.
         */
        var input: ByteArray? = null,
        /**
         * Enables logging in debug mode.
         */
        var isDebug: Boolean = true,
        /**
         * Validation mode. Supported: json2avro, avro2json, json2avro2json.
         */
        var mode: String = "json2avro",
        /**
         * Displays this help message.
         */
        var isHelp: Boolean = false
)
