/*
 * Copyright (c) 2021 AtLarge Research
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

package org.opendc.trace.spi

import org.opendc.trace.Trace
import java.net.URL
import java.util.*

/**
 * A service-provider class for parsing trace formats.
 */
public interface TraceFormat {
    /**
     * The name of the trace format.
     */
    public val name: String

    /**
     * Open a new [Trace] with this provider.
     *
     * @param url A reference to the trace.
     */
    public fun open(url: URL): Trace

    /**
     * A helper object for resolving providers.
     */
    public companion object {
        /**
         * A list of [TraceFormat] that are available on this system.
         */
        public val installedProviders: List<TraceFormat> by lazy {
            val loader = ServiceLoader.load(TraceFormat::class.java)
            loader.toList()
        }

        /**
         * Obtain a [TraceFormat] implementation by [name].
         */
        public fun byName(name: String): TraceFormat? = installedProviders.find { it.name == name }
    }
}
