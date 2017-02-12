package org.sl.utils.avro.converter

import org.apache.avro.AvroRuntimeException

class AvroConversionException(message: String, cause: Throwable) : AvroRuntimeException(message, cause)