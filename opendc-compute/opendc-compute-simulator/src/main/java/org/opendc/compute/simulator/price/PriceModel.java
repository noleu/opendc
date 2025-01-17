/*
 * Copyright (c) 2024 AtLarge Research
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

package org.opendc.compute.simulator.price;

import java.util.List;

import org.opendc.compute.simulator.host.SimHost;
//import org.opendc.simulator.engine.graph.FlowDistributor;
import org.opendc.simulator.engine.graph.FlowGraph;
import org.opendc.simulator.engine.graph.FlowNode;

/**
 * PriceModel used to provide the Price of a {@link SimHost}
 * A PriceModel is based on a list of {@link PriceFragment} that define the price at specific time frames.
 */
public class PriceModel extends FlowNode {

    private SimHost host;

    private long startTime = 0L; // The absolute timestamp on which the workload started

    private List<PriceFragment> fragments;
    private PriceFragment current_fragment;

    private int fragment_index;

    /**
     * Construct a PriceModel
     *
     * @param parentGraph The active FlowGraph which should be used to make the new FlowNode
     * @param host The Host which should be updated with the price
     * @param priceFragments A list of Price Fragments defining the price at different time frames
     * @param startTime The start time of the simulation. This is used to go from relative time (used by the clock)
     *                  to absolute time (used by price fragments).
     */
    public PriceModel(
        FlowGraph parentGraph, SimHost host, List<PriceFragment> priceFragments, long startTime) {
        super(parentGraph);

        this.host = host;
        this.startTime = startTime;
        this.fragments = priceFragments;

        this.fragment_index = 0;
        this.current_fragment = this.fragments.get(this.fragment_index);

        this.pushPriceState(this.current_fragment.getOnDemandPrice(), this.current_fragment.getSpotPrice());
        this.pushPrice(this.current_fragment.getOnDemandPrice(), this.current_fragment.getSpotPrice());

    }

    public void close() {
        this.closeNode();
    }

    /**
     * Convert the given relative time to the absolute time by adding the start of workload
     */
    private long getAbsoluteTime(long time) {
        return time + startTime;
    }

    /**
     * Convert the given absolute time to the relative time by subtracting the start of workload
     */
    private long getRelativeTime(long time) {
        return time - startTime;
    }

    /**
     * Traverse the fragments to find the fragment that matches the given absoluteTime
     */
    private void findCorrectFragment(long absoluteTime) {

        // Traverse to the previous fragment, until you reach the correct fragment
        while (absoluteTime < this.current_fragment.getStartTime() && this.fragment_index > 0) {
            this.current_fragment = fragments.get(--this.fragment_index);
        }

        // Traverse to the next fragment, until you reach the correct fragment
        while (absoluteTime >= this.current_fragment.getEndTime() && this.fragment_index < this.fragments.size() - 1) {
            this.current_fragment = fragments.get(++this.fragment_index);
        }

        if (this.fragment_index < 0 || this.fragment_index >= this.fragments.size() - 1) {
            close();
        }
    }

    @Override
    public long onUpdate(long now) {
        long absolute_time = getAbsoluteTime(now);

        // Check if the current fragment is still the correct fragment,
        // Otherwise, find the correct fragment.
        if ((absolute_time < current_fragment.getStartTime()) || (absolute_time >= current_fragment.getEndTime())) {
            this.findCorrectFragment(absolute_time);

            pushPriceState(current_fragment.getOnDemandPrice(), current_fragment.getSpotPrice());
            pushPrice(current_fragment.getOnDemandPrice(), current_fragment.getSpotPrice());

        }

        // Update again at the end of this fragment
        return getRelativeTime(current_fragment.getEndTime());
    }

    private void pushPriceState(double onDemandPrice, double spotPrice) {
        PriceState state;

        if (spotPrice >= onDemandPrice) {
            state = PriceState.ON_DEMAND;
        } else {
            state = PriceState.SPOT;
        }

        this.host.updatePriceState(state);
    }

    private void pushPrice(double onDemandPrice, double spotPrice) {
        this.host.updatePrice(onDemandPrice, spotPrice);
    }
}
