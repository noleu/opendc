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

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.opendc.trace.conv.PRICE_TIMESTAMP
import org.opendc.trace.conv.PRICE_ON_DEMAND
import org.opendc.trace.conv.PRICE_SPOT

/**
 * A [ReadSupport] instance for [Task] objects.
 *
 * @param projection The projection of the table to read.
 */
internal class PriceReadSupport(private val projection: List<String>?) : ReadSupport<PriceFragment>() {
    /**
     * Mapping of table columns to their Parquet column names.
     */
    private val colMap =
        mapOf(
            PRICE_TIMESTAMP to "timestamp",
            PRICE_ON_DEMAND to "on_demand_price",
            PRICE_SPOT to "spot_price",
        )

    override fun init(context: InitContext): ReadContext {
        val projectedSchema =
            if (projection != null) {
                Types.buildMessage()
                    .apply {
                        val fieldByName = READ_SCHEMA.fields.associateBy { it.name }

                        for (col in projection) {
                            val fieldName = colMap[col] ?: continue
                            addField(fieldByName.getValue(fieldName))
                        }
                    }
                    .named(READ_SCHEMA.name)
            } else {
                READ_SCHEMA
            }
        return ReadContext(projectedSchema)
    }

    override fun prepareForRead(
        configuration: Configuration,
        keyValueMetaData: Map<String, String>,
        fileSchema: MessageType,
        readContext: ReadContext,
    ): RecordMaterializer<PriceFragment> =PriceRecordMaterializer(readContext.requestedSchema)

    companion object {
        /**
         * Parquet read schema for the "price" table in the trace.
         */
        @JvmStatic
        val READ_SCHEMA: MessageType =
            Types.buildMessage()
                .addFields(
                    Types
                        .optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named("timestamp"),
                    Types
                        .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named("on_demand_price"),
                    Types
                        .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
                        .named("spot_price")
                )
                .named("price_fragment")
    }
}
