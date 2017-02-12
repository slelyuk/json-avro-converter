package org.sl.utils.avro.validator

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.sl.utils.avro.validator.test.ResourceUtils.Companion.readResource
import kotlin.test.assertFailsWith

class ValidatorRunnerSpec : Spek({

    describe("ValidatorRunnerSpec") {
        it("should validate JSON document against the schema") {
            val options: ValidatorOptions = ValidatorOptions(
                    schema = readResource("user.avcs"),
                    input = readResource("user.json")
            )

            ValidatorRunner.validate(options)
        }

        it("should report JSON validation errors") {
            val options: ValidatorOptions = ValidatorOptions(
                    schema = readResource("user.avcs"),
                    input = readResource("invalid_user.json")
            )

            assertFailsWith(ValidatorException::class) {
                ValidatorRunner.validate(options)
            }
        }

        it("should validate AVRO document against the schema") {
            val options = ValidatorOptions(
                    schema = readResource("user.avcs"),
                    input = readResource("user.avro"),
                    mode = "avro2json")

            ValidatorRunner.validate(options)
        }
    }
})
