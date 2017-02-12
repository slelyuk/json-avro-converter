package org.sl.utils.avro.converter

import org.apache.avro.AvroTypeException
import java.util.*
import java.util.Spliterator.ORDERED
import java.util.Spliterators.spliteratorUnknownSize
import java.util.stream.Collectors.joining
import java.util.stream.StreamSupport

class TypeExceptions {
    companion object Avro {
        fun enumException(fieldPath: Deque<String>, expectedSymbols: String): AvroTypeException {
            return AvroTypeException(StringBuilder().append("Field ").append(path(fieldPath)).append(" is expected to be of enum type and be one of ").append(expectedSymbols).toString())
        }

        fun unionException(fieldName: String, expectedTypes: String, offendingPath: Deque<String>): AvroTypeException {
            return AvroTypeException(StringBuilder().append("Could not evaluate union, field").append(fieldName).append("is expected to be one of these: ").append(expectedTypes).append("If this is a complex type, check if offending field: ").append(path(offendingPath)).append(" adheres to schema.").toString())
        }

        fun typeException(fieldPath: Deque<String>, expectedType: String): AvroTypeException {
            return AvroTypeException(StringBuilder().append("Field ").append(path(fieldPath)).append(" is expected to be type: ").append(expectedType).toString())
        }

        private fun path(path: Deque<String>): String {
            return StreamSupport.stream(spliteratorUnknownSize(path.descendingIterator(), ORDERED), false).map { it.toString() }.collect(joining("."))
        }
    }
}
