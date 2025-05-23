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

package org.opendc.simulator.compute.power;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opendc.simulator.engine.engine.FlowEngine;
import org.opendc.simulator.engine.graph.FlowEdge;
import org.opendc.simulator.engine.graph.FlowNode;

/**
 * CarbonModel used to provide the Carbon Intensity of a {@link SimPowerSource}
 * A CarbonModel is based on a list of {@link CarbonFragment} that define the carbon intensity at specific time frames.
 */
public class CarbonModel extends FlowNode {

    private final ArrayList<CarbonReceiver> receivers = new ArrayList<>();

    private final long startTime; // The absolute timestamp on which the workload started

    private final List<CarbonFragment> fragments;
    private CarbonFragment current_fragment;

    private int fragment_index;

    /**
     * Construct a CarbonModel
     *
     * @param engine The {@link FlowEngine} the node belongs to
     * @param carbonFragments A list of Carbon Fragments defining the carbon intensity at different time frames
     * @param startTime The start time of the simulation. This is used to go from relative time (used by the clock)
     *                  to absolute time (used by carbon fragments).
     */
    public CarbonModel(FlowEngine engine, List<CarbonFragment> carbonFragments, long startTime) {
        super(engine);

        this.startTime = startTime;
        this.fragments = carbonFragments;

        this.fragment_index = 0;
        this.current_fragment = this.fragments.get(this.fragment_index);
        this.pushCarbonIntensity(this.current_fragment.getCarbonIntensity());
    }

    public void close() {
        for (CarbonReceiver receiver : receivers) {
            receiver.removeCarbonModel(this);
        }

        receivers.clear();

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
        while (absoluteTime < this.current_fragment.getStartTime()) {
            this.current_fragment = fragments.get(--this.fragment_index);
        }

        // Traverse to the next fragment, until you reach the correct fragment
        while (absoluteTime >= this.current_fragment.getEndTime()) {
            this.current_fragment = fragments.get(++this.fragment_index);
        }
    }

    @Override
    public long onUpdate(long now) {
        long absolute_time = getAbsoluteTime(now);

        // Check if the current fragment is still the correct fragment,
        // Otherwise, find the correct fragment.
        if ((absolute_time < current_fragment.getStartTime()) || (absolute_time >= current_fragment.getEndTime())) {
            this.findCorrectFragment(absolute_time);

            pushCarbonIntensity(current_fragment.getCarbonIntensity());
        }

        // Update again at the end of this fragment
        return getRelativeTime(current_fragment.getEndTime());
    }

    private void pushCarbonIntensity(double carbonIntensity) {
        for (CarbonReceiver receiver : this.receivers) {
            receiver.updateCarbonIntensity(carbonIntensity);
        }
    }

    public void addReceiver(CarbonReceiver receiver) {
        this.receivers.add(receiver);

        receiver.setCarbonModel(this);

        receiver.updateCarbonIntensity(this.current_fragment.getCarbonIntensity());
    }

    public double[] getForecast(int forecastSize) {
        return this.fragments
                .subList(
                        Math.min(this.fragment_index + 1, this.fragments.size() - 1),
                        Math.min(this.fragment_index + forecastSize, this.fragments.size()))
                .stream()
                .mapToDouble(CarbonFragment::getCarbonIntensity)
                .toArray();
    }

    public static <T, U> List<U> castList(List<T> list, Class<U> clazz) {
        List<U> result = new ArrayList<>();
        for (T element : list) {
            result.add(clazz.cast(element));
        }
        return result;
    }

    @Override
    public Map<FlowEdge.NodeType, List<FlowEdge>> getConnectedEdges() {
        return Map.of();
    }
}
