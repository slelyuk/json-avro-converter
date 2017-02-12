package org.sl.utils.avro.validator

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.sl.utils.avro.validator.ValidationMode.AVRO_TO_JSON
import org.sl.utils.avro.validator.test.AvroUtils
import org.sl.utils.avro.validator.test.ResourceUtils.Companion.readResource
import java.io.ByteArrayInputStream
import kotlin.test.assertFailsWith

class AvroValidatorSpec : Spek({
    describe("AvroValidatorSpec") {
        var validator: AvroValidator
        val avroSchema: ByteArray = readResource("user.avcs")
        val schema: Schema = Schema.Parser().parse(ByteArrayInputStream(avroSchema))

        it("should validate JSON document against the schema") {
            validator = Validators.avro()
                    .withInput(readResource("user.json"))
                    .withSchema(avroSchema)
                    .build()
            validator.validate()
        }

        it("should report JSON validation errors") {
            validator = Validators.avro()
                    .withInput(readResource("invalid_user.json"))
                    .withSchema(avroSchema)
                    .build()
            assertFailsWith(ValidatorException::class) {
                validator.validate()
            }
        }

        it("should validate AVRO document against the schema") {
            val user = GenericData.Record(schema)
            user.put("name", "Bob")
            user.put("age", 50)
            user.put("favoriteColor", "blue")
            validator = Validators.avro()
                    .withMode(AVRO_TO_JSON)
                    .withInput(AvroUtils.Companion.recordToBytes(user, schema))
                    .withSchema(avroSchema)
                    .build()
            val result: ValidationResult = validator.validate()
            assert(result.output.isPresent)
        }

        it("should validate conversion from JSON to AVRO and back to JSON") {
            validator = Validators.avro()
                    .withInput(readResource("user.json"))
                    .withMode(ValidationMode.JSON_TO_AVRO_TO_JSON)
                    .withSchema(avroSchema)
                    .build()
            val result: ValidationResult = validator.validate()
            val json: Map<String, *> = jacksonObjectMapper().readValue(result.output.get())
            assert(json["name"] == "Bob")
        }
    }
})
