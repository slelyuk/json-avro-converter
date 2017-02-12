package org.apache.avro.io

import org.apache.avro.Schema
import org.apache.avro.io.parsing.Symbol
import org.codehaus.jackson.JsonGenerator
import java.io.IOException
import java.io.OutputStream

class NoWrappingJsonEncoder : JsonEncoder {
    @Throws(IOException::class)
    constructor(sc: Schema, out: OutputStream) : super(sc, out)

    @Throws(IOException::class)
    constructor(sc: Schema, out: OutputStream, pretty: Boolean) : super(sc, out, pretty)

    @Throws(IOException::class)
    constructor(sc: Schema, out: JsonGenerator) : super(sc, out)

    @Throws(IOException::class)
    override fun writeIndex(unionIndex: Int) {
        parser.advance(Symbol.UNION)
        val top = parser.popSymbol() as Symbol.Alternative
        val symbol = top.getSymbol(unionIndex)
        parser.pushSymbol(symbol)
    }
}