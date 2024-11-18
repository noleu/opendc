package org.opendc.simulator.compute.price;

import org.opendc.simulator.compute.cpu.SimCpu;
import org.opendc.simulator.engine.FlowEdge;
import org.opendc.simulator.engine.FlowGraph;
import org.opendc.simulator.engine.FlowNode;
import org.opendc.simulator.engine.FlowSupplier;

import java.util.List;

/**
 * A {@link SimPriceSource} implementation that estimates the power consumption based on CPU usage.
 */
public final class SimPriceSource extends FlowNode implements FlowSupplier {
    private long lastUpdate;

    private double currentPrice = 0.0f;
    private double totalPrice = 0.0f;

    private PriceModel priceModel = null;
    private FlowEdge muxEdge;

    private double capacity = Long.MAX_VALUE;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Basic Getters and Setters
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Determine whether the InPort is connected to a {@link SimCpu}.
     *
     * @return <code>true</code> if the InPort is connected to an OutPort, <code>false</code> otherwise.
     */
    public boolean isConnected() {
        return muxEdge != null;
    }


    public double getCurrentPrice() {
        return this.currentPrice;
    }

    public double getCarbonEmission() {
        return this.totalPrice;
    }

    @Override
    public double getCapacity() {
        return this.capacity;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SimPriceSource(FlowGraph graph, double max_capacity, List<PriceFragment> priceFragments, long startTime) {
        super(graph);

        this.capacity = max_capacity;

        if (priceFragments != null) {
            this.priceModel = new PriceModel(graph, this, priceFragments, startTime);
        }
        lastUpdate = this.clock.millis();
    }

    public void close() {
        if (this.priceModel != null) {
            this.priceModel.close();
            this.priceModel = null;
        }

        this.closeNode();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // FlowNode related functionality
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public long onUpdate(long now) {
        updateCounters();
        //double powerSupply = this.powerDemand;

        //if (powerSupply != this.powerSupplied) {
        //    this.pushSupply(this.muxEdge, powerSupply);
        //}

        return Long.MAX_VALUE;
    }

    public void updateCounters() {
        updateCounters(clock.millis());
    }

    /**
     * Calculate the energy usage up until <code>now</code>.
     */
    public void updateCounters(long now) {
        long lastUpdate = this.lastUpdate;
        this.lastUpdate = now;

        long duration = now - lastUpdate;
        if (duration > 0) {
            double pricePerUsage = (this.currentPrice * duration * 0.001);

            // Compute the energy usage of the machine
            this.totalPrice += pricePerUsage;
            //this.totalPrice += this.currentPrice * (energyUsage / 3600000.0);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // FlowGraph Related functionality
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void handleDemand(FlowEdge consumerEdge, double newPowerDemand) {

        //this.powerDemand = newPowerDemand;
        this.invalidate();
    }

    @Override
    public void pushSupply(FlowEdge consumerEdge, double newSupply) {

        //this.powerSupplied = newSupply;
        consumerEdge.pushSupply(newSupply);
    }

    @Override
    public void addConsumerEdge(FlowEdge consumerEdge) {
        this.muxEdge = consumerEdge;
    }

    @Override
    public void removeConsumerEdge(FlowEdge consumerEdge) {
        this.muxEdge = null;
    }

    // Update the carbon intensity of the power source
    public void updatePrice(double newPrice) {
        this.updateCounters();
        this.currentPrice = newPrice;
    }
}
