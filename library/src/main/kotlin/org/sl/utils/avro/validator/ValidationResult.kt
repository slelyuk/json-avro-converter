package org.sl.utils.avro.validator

import com.google.common.base.MoreObjects
import java.util.*

class ValidationResult(val output: Optional<String>) {

    override fun hashCode(): Int {
        return Objects.hash(output)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as ValidationResult?
        return output == that!!.output
    }

    override fun toString(): String {
        return MoreObjects.toStringHelper(this)
                .add("output", output)
                .toString()
    }

    companion object {
        fun success(): ValidationResult {
            return ValidationResult(Optional.empty<String>())
        }

        fun success(output: String): ValidationResult {
            return ValidationResult(Optional.ofNullable(output))
        }
    }
}
