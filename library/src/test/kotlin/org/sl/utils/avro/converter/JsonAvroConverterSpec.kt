package org.sl.utils.avro.converter

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class JsonAvroConverterSpec : Spek({
    describe("JsonAvroConverter") {
        val converter = JsonAvroConverter()

        it("should convert record with primitives") {
            val schema = """
            {
              "type" : "record",
              "name" : "testSchema",
              "fields" : [
                  {
                    "name" : "field_integer",
                    "type" : "int"
                  },
                  {
                    "name" : "field_long",
                    "type" : "long"
                  },
                  {
                    "name" : "field_float",
                    "type" : "float"
                  },
                  {
                    "name" : "field_double",
                    "type" : "double"
                  },
                  {
                    "name" : "field_boolean",
                    "type" : "boolean"
                  },
                  {
                    "name" : "field_string",
                    "type" : "string"
                  }
              ]
            }
            """

            val json = """
            {
                "field_integer": 1,
                "field_long": 2,
                "field_float": 1.1,
                "field_double": 1.2,
                "field_boolean": true,
                "field_string": "foobar"
            }
            """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assert(toMap(json) == toMap(converter.convertToJson(avro, schema)))
        }

        it("should throw exception when parsing record with mismatched primitives") {

            val schema = """
            {
                "type" : "record",
                "name" : "testSchema",
                "fields" : [
                {
                    "name" : "field_integer",
                    "type" : "int"
                }
                ]
            }"""

            val json = """
            {
                "field_integer": "foobar"
            }"""

            assertFailsWith(AvroConversionException::class) {
                converter.convertToAvro(json.toByteArray(), schema)
            }
        }

        it("should ignore unknown fields") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_string",
                "type" : "string"
            }
            ]
        }
        """

            val json = """
        {
            "field_integer": 1,
            "field_long": 2,
            "field_float": 1.1,
            "field_double": 1.2,
            "field_boolean": true,
            "field_string": "foobar"
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            val result = toMap(converter.convertToJson(avro, schema))
            assertEquals(result["field_string"], "foobar")
            assert(result.keys.size == 1)
        }

        it("should throw exception when field is missing") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_integer",
                "type" : "int"
            },
            {
                "name" : "field_long",
                "type" : "long"
            }
            ]
        }
        """

            val json = """
        {
            "field_integer": 1
        }
        """

            assertFailsWith(AvroConversionException::class) {
                converter.convertToAvro(json.toByteArray(), schema)
            }
        }

        it("should convert message with nested records") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_record",
                "type" : {
                "name" : "string_type",
                "type": "record",
                "fields": [
                {
                    "type": "string",
                    "name": "field_string"
                }
                ]
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_record": {
                "field_string": "foobar"
                }
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assertEquals(toMap(json), toMap(converter.convertToJson(avro, schema)))
        }

        it("should convert nested record with missing field") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_record",
                "type" : {
                "name" : "string_type",
                "type": "record",
                "fields": [
                {
                    "type": "string",
                    "name": "field_string"
                }
                ]
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_foobar": 1
        }
        """

            assertFailsWith(AvroConversionException::class) {
                converter.convertToAvro(json.toByteArray(), schema)
            }
        }

        it("should convert nested map of primitives") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_map",
                "type" : {
                "name" : "map_type",
                "type": "map",
                "values": "string"
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_map": {
            "foo": "bar"
        }
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assertEquals(toMap(json), toMap(converter.convertToJson(avro, schema)))
        }

        it("should fail when converting nested map with mismatched value type") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_map",
                "type" : {
                "name" : "map_type",
                "type": "map",
                "values": "string"
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_map": 1
        }
        """

            assertFailsWith(AvroConversionException::class) {
                converter.convertToAvro(json.toByteArray(), schema)
            }
        }

        it("should convert nested map of records") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_map",
                "type" : {
                "name" : "map_type",
                "type": "map",
                "values": {
                "name" : "string_type",
                "type": "record",
                "fields": [
                {
                    "type": "string",
                    "name": "field_string"
                }
                ]
            }
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_map": {
            "foo": {
            "field_string": "foobar"
        }
        }
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assertEquals(toMap(json), toMap(converter.convertToJson(avro, schema)))
        }

        it("should convert nested array of primitives") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_array",
                "type" : {
                "name" : "array_type",
                "type": "array",
                "items": {
                "name": "item",
                "type": "string"
            }
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_array": ["foo", "bar"]
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assertEquals(toMap(json), toMap(converter.convertToJson(avro, schema)))
        }

        it("should convert nested array of records") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_array",
                "type" : {
                "name" : "array_type",
                "type": "array",
                "items": {
                "name" : "string_type",
                "type": "record",
                "fields": [
                {
                    "type": "string",
                    "name": "field_string"
                }
                ]
            }
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_array": [
            {
                "field_string": "foo"
            },
            {
                "field_string": "bar"
            }
            ]
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assertEquals(toMap(json), toMap(converter.convertToJson(avro, schema)))
        }

        it("should convert nested union of primitives") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_union",
                "type" : ["string", "int"]
            }
            ]
        }
        """

            val json = """
        {
            "field_union": 8
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assertEquals(toMap(json), toMap(converter.convertToJson(avro, schema)))
        }

        it("should convert nested union of records") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_union",
                "type" : [
                {
                    "name" : "int_type",
                    "type": "record",
                    "fields": [
                    {
                        "type": "int",
                        "name": "field_int"
                    }
                    ]
                },
                {
                    "name" : "string_type",
                    "type": "record",
                    "fields": [
                    {
                        "type": "string",
                        "name": "field_string"
                    }
                    ]
                }
                ]
            }
            ]
        }
        """

            val json = """
        {
            "field_union": {
            "field_string": "foobar"
        }
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            assertEquals(toMap(json), toMap(converter.convertToJson(avro, schema)))
        }

        it("should convert nested union with null and primitive should result in an optional field") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_string",
                "type" : "string"
            },
            {
                "name" : "field_union",
                "type" : ["null", "int"],
                "default": null
            }
            ]
        }
        """

            val json = """
        {
            "field_string": "foobar"
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            val result = toMap(converter.convertToJson(avro, schema))
            assert(result["field_string"] == toMap(json)["field_string"])
            assert(result["field_union"] == null)
        }

        it("should convert nested union with null and record should result in an optional field") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_string",
                "type" : "string"
            },
            {
                "name" : "field_union",
                "type" : [
                "null",
                {
                    "name" : "int_type",
                    "type": "record",
                    "fields": [
                    {
                        "type": "int",
                        "name": "field_int"
                    }
                    ]
                }
                ],
                "default": null
            }
            ]
        }
        """

            val json = """
        {
            "field_string": "foobar"
        }
        """

            val avro: ByteArray = converter.convertToAvro(json.toByteArray(), schema)
            val result: Map<String, *> = toMap(converter.convertToJson(avro, schema))
            assert(result["field_string"] == toMap(json)["field_string"])
            assert(result["field_union"] == null)
        }

        it("should convert nested union with null and map should result in an optional field") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_string",
                "type" : "string"
            },
            {
                "name" : "field_union",
                "type" : [
                "null",
                {
                    "name" : "map_type",
                    "type": "map",
                    "values": {
                    "name" : "string_type",
                    "type": "record",
                    "fields": [
                    {
                        "type": "string",
                        "name": "field_string"
                    }
                    ]
                }
                }
                ],
                "default": null
            }
            ]
        }
        """

            val json = """
        {
            "field_string": "foobar"
        }
        """

            val avro = converter.convertToAvro(json.toByteArray(), schema)
            val result = toMap(converter.convertToJson(avro, schema))
            assert(result["field_string"] == toMap(json)["field_string"])
            assert(result["field_union"] == null)
        }

        it("should convert optional fields should not be wrapped when converting from avro to json") {

            val schema = """
        {
            "type" : "record",
            "name" : "testSchema",
            "fields" : [
            {
                "name" : "field_string",
                "type" : "string"
            },
            {
                "name" : "field_union",
                "type" : ["null", "int"],
                "default": null
            }
            ]
        }
        """

            val json = """
        {
            "field_string": "foobar",
            "field_union": 1
        }
        """

            val result = converter.convertToJson(converter.convertToAvro(json.toByteArray(), schema), schema)
            assert(toMap(result) == toMap(json))
        }

        it("should print full path to invalid field on error") {

            val schema = """
        {
            "name": "testSchema",
            "type": "record",
            "fields": [
            {
                "name": "field",
                "default": null,
                "type": [
                "null",
                {
                    "name": "field_type",
                    "type": "record",
                    "fields": [
                    {
                        "name": "stringValue",
                        "type": "string"
                    }
                    ]
                }
                ]
            }
            ]
        }
        """

            val json = """
        {
            "field": { "stringValue": 1 }
        }
        """

            try {
                converter.convertToJson(converter.convertToAvro(json.toByteArray(), schema), schema)
            } catch (e: AvroConversionException) {
                assertRegex("/.*field\\.stringValue.*/", e.cause?.message)
            }
        }

        it("should parse enum types properly") {

            val schema = """
        {
            "name": "testSchema",
            "type": "record",
            "fields": [
            {
                "name" : "field_enum",
                "type" : {
                "name" : "MyEnums",
                "type" : "enum",
                "symbols" : [ "A", "B", "C" ]
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_enum": "A"
        }
        """

            val result = converter.convertToJson(converter.convertToAvro(json.toByteArray(), schema), schema)
            assert(toMap(result) == toMap(json))
        }

        it("should throw the apprioriate error when passing an invalid enum type") {

            val schema = """
        {
            "name": "testSchema",
            "type": "record",
            "fields": [
            {
                "name" : "field_enum",
                "type" : {
                "name" : "MyEnums",
                "type" : "enum",
                "symbols" : [ "A", "B", "C" ]
            }
            }
            ]
        }
        """

            val json = """
        {
            "field_enum": "D"
        }
        """

            assertFailsWith(AvroConversionException::class) {
                converter.convertToJson(converter.convertToAvro(json.toByteArray(), schema), schema)
            }

            try {
                converter.convertToJson(converter.convertToAvro(json.toByteArray(), schema), schema)
            } catch (e: AvroConversionException) {
                assertRegex("/.*enum type and be one of A, B, C.*/", e.cause?.message)
            }

            //then:
            //val exception = thrown(AvroConversionException)
            //exception.cause.message ==~ /.*enum type and be one of A, B, C.*/
        }

    }
})

fun toMap(jsonBytes: ByteArray): Map<String, *> {
    return jacksonObjectMapper().readValue(jsonBytes)
}

fun toMap(jsonString: String): Map<String, *> {
    return jacksonObjectMapper().readValue(jsonString)
}

fun assertRegex(literal: String, value: String?): Boolean {
    return Regex.fromLiteral(literal).containsMatchIn(value!!)
}