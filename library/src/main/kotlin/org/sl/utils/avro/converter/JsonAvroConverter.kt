package org.sl.utils.avro.converter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.NoWrappingJsonEncoder
import java.io.ByteArrayOutputStream
import java.io.IOException

class JsonAvroConverter {
    private var recordReader: JsonGenericRecordReader

    fun convertToAvro(data: ByteArray, schema: String): ByteArray {
        return convertToAvro(data, Schema.Parser().parse(schema))
    }

    fun convertToAvro(data: ByteArray, schema: Schema): ByteArray {
        try {
            val outputStream = ByteArrayOutputStream()
            val encoder = EncoderFactory.get().binaryEncoder(outputStream, null)
            val writer = GenericDatumWriter<Any>(schema)
            writer.write(convertToGenericDataRecord(data, schema), encoder)
            encoder.flush()
            return outputStream.toByteArray()
        } catch (e: IOException) {
            throw AvroConversionException("Failed to convert to AVRO.", e)
        }

    }

    fun convertToGenericDataRecord(data: ByteArray, schema: Schema): GenericData.Record {
        return recordReader.read(data, schema)
    }

    fun convertToJson(avro: ByteArray, schema: String): ByteArray {
        return convertToJson(avro, Schema.Parser().parse(schema))
    }

    fun convertToJson(avro: ByteArray, schema: Schema): ByteArray {
        try {
            val binaryDecoder = DecoderFactory.get().binaryDecoder(avro, null)
            val record = GenericDatumReader<GenericRecord>(schema).read(null, binaryDecoder)
            return convertToJson(record)
        } catch (e: IOException) {
            throw AvroConversionException("Failed to create avro structure.", e)
        }

    }

    fun convertToJson(record: GenericRecord): ByteArray {
        try {
            val outputStream = ByteArrayOutputStream()
            val jsonEncoder = NoWrappingJsonEncoder(record.schema, outputStream)
            GenericDatumWriter<GenericRecord>(record.schema).write(record, jsonEncoder)
            jsonEncoder.flush()
            return outputStream.toByteArray()
        } catch (e: IOException) {
            throw AvroConversionException("Failed to convert to JSON.", e)
        }

    }

    init {
        this.recordReader = JsonGenericRecordReader()
    }
}
