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

/**
 * An object holding the carbon intensity during a specific time frame.
 * Used by {@link PriceModel}.
 */
public class PriceFragment {
    private long startTime;
    private long endTime;

    private double onDemandPrice;
    private double spotPrice;

    public PriceFragment(long startTime, long endTime, double onDemandPrice, double spotPrice) {
        this.setStartTime(startTime);
        this.setEndTime(endTime);
        this.setOnDemandPrice(onDemandPrice);
        this.setSpotPrice(spotPrice);
    }

    public double getOnDemandPrice() {
        return onDemandPrice;
    }

    public void setOnDemandPrice(double price) {
        this.onDemandPrice = price;
    }

    public double getSpotPrice() {return spotPrice;}

    public void setSpotPrice(double price) {this.spotPrice = price;}

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
}
