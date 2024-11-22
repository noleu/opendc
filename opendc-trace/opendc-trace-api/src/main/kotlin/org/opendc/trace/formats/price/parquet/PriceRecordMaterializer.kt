/*
 * Copyright (c) 2022 AtLarge Research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.opendc.trace.formats.price.parquet

import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType
import java.time.Instant

/**
 * A [RecordMaterializer] for [Task] records.
 */
internal class PriceRecordMaterializer(schema: MessageType) : RecordMaterializer<PriceFragment>() {
    /**
     * State of current record being read.
     */
    private var localTimestamp: Instant = Instant.MIN
    private var localPrice: Double = 0.0

    /**
     * Root converter for the record.
     */
    private val root =
        object : GroupConverter() {
            /**
             * The converters for the columns of the schema.
             */
            private val converters =
                schema.fields.map { type ->
                    when (type.name) {
                        "timestamp" ->
                            object : PrimitiveConverter() {
                                override fun addLong(value: Long) {
                                    localTimestamp = Instant.ofEpochMilli(value)
                                }
                            }
                        "price" ->
                            object : PrimitiveConverter() {
                                override fun addDouble(value: Double) {
                                    localPrice = value
                                }
                            }
                        else -> error("Unknown column $type")
                    }
                }

            override fun start() {
                localTimestamp = Instant.MIN
                localPrice = 0.0
            }

            override fun end() {}

            override fun getConverter(fieldIndex: Int): Converter = converters[fieldIndex]
        }

    override fun getCurrentRecord(): PriceFragment =
        PriceFragment(
            localTimestamp,
            localPrice,
        )

    override fun getRootConverter(): GroupConverter = root
}
