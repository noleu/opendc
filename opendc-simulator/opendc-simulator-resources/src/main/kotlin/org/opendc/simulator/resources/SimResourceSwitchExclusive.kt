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

package org.opendc.simulator.resources

import org.opendc.simulator.resources.interference.InterferenceKey
import java.util.ArrayDeque

/**
 * A [SimResourceSwitch] implementation that allocates outputs to the inputs of the switch exclusively. This means that
 * a single output is directly connected to an input and that the switch can only support as much outputs as inputs.
 */
public class SimResourceSwitchExclusive : SimResourceSwitch {
    /**
     * A flag to indicate that the switch is closed.
     */
    private var isClosed: Boolean = false

    private val _outputs = mutableSetOf<Provider>()
    override val outputs: Set<SimResourceCloseableProvider>
        get() = _outputs

    private val availableResources = ArrayDeque<SimResourceTransformer>()

    private val _inputs = mutableSetOf<SimResourceProvider>()
    override val inputs: Set<SimResourceProvider>
        get() = _inputs

    override val counters: SimResourceCounters = object : SimResourceCounters {
        override val demand: Double
            get() = _inputs.sumOf { it.counters.demand }
        override val actual: Double
            get() = _inputs.sumOf { it.counters.actual }
        override val overcommit: Double
            get() = _inputs.sumOf { it.counters.overcommit }

        override fun reset() {
            for (input in _inputs) {
                input.counters.reset()
            }
        }

        override fun toString(): String = "SimResourceCounters[demand=$demand,actual=$actual,overcommit=$overcommit]"
    }

    /**
     * Add an output to the switch.
     */
    override fun newOutput(key: InterferenceKey?): SimResourceCloseableProvider {
        check(!isClosed) { "Switch has been closed" }
        check(availableResources.isNotEmpty()) { "No capacity to serve request" }
        val forwarder = availableResources.poll()
        val output = Provider(forwarder)
        _outputs += output
        return output
    }

    override fun addInput(input: SimResourceProvider) {
        check(!isClosed) { "Switch has been closed" }

        if (input in inputs) {
            return
        }

        val forwarder = SimResourceForwarder()

        _inputs += input
        availableResources += forwarder

        input.startConsumer(object : SimResourceConsumer by forwarder {
            override fun onEvent(ctx: SimResourceContext, event: SimResourceEvent) {
                if (event == SimResourceEvent.Exit) {
                    // De-register the input after it has finished
                    _inputs -= input
                }

                forwarder.onEvent(ctx, event)
            }
        })
    }

    override fun close() {
        isClosed = true

        // Cancel all upstream subscriptions
        _inputs.forEach(SimResourceProvider::cancel)
    }

    private inner class Provider(private val forwarder: SimResourceTransformer) : SimResourceCloseableProvider, SimResourceProvider by forwarder {
        override fun close() {
            // We explicitly do not close the forwarder here in order to re-use it across output resources.
            _outputs -= this
            availableResources += forwarder
        }
    }
}
