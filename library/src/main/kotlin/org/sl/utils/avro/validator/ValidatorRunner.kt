package org.sl.utils.avro.validator

import ch.qos.logback.classic.Level
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ValidatorRunner {
    private val log = LoggerFactory.getLogger(ValidatorRunner::class.java)

    @Throws(ValidatorException::class)
    fun validate(options: ValidatorOptions) {
        try {
            configureLogging(options)
            Validators.avro()
                    .withMode(getMode(options))
                    .withInput(options.input)
                    .withSchema(options.schema)
                    .build()
                    .validate()
        } catch (e: ValidatorException) {
            log.error("Document could not be validated", e)
            throw e
        }

    }

    private fun getMode(options: ValidatorOptions): ValidationMode {
        return ValidationMode.from(options.mode)
    }

    private fun configureLogging(options: ValidatorOptions) {
        if (isDebugEnabled(options)) {
            (LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger).level = Level.DEBUG
        }
    }

    private fun isDebugEnabled(options: ValidatorOptions): Boolean {
        return options.isDebug && LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) is ch.qos.logback.classic.Logger
    }
}
