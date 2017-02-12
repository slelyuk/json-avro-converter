package org.sl.utils.avro.validator.test

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream

class AvroUtils {

    companion object {
        fun recordToBytes(record: GenericRecord, schema: Schema): ByteArray {
            val out: ByteArrayOutputStream = ByteArrayOutputStream()
            val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
            val writer: DatumWriter<GenericRecord> = GenericDatumWriter(schema)
            writer.write(record, encoder)
            encoder.flush()
            return out.toByteArray()
        }
    }
}