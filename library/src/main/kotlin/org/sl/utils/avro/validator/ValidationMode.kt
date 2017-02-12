package org.sl.utils.avro.validator

import java.util.*

enum class ValidationMode {
    JSON_TO_AVRO,
    AVRO_TO_JSON,
    JSON_TO_AVRO_TO_JSON;


    companion object {
        fun from(name: String): ValidationMode {
            return Arrays.stream(values())
                    .filter { value ->
                        value.name
                                .replace("_".toRegex(), "")
                                .replace("TO".toRegex(), "2")
                                .equals(name, ignoreCase = true)
                    }
                    .findFirst()
                    .get()
        }
    }
}
