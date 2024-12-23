package org.opendc.compute.simulator.scheduler.filters

import org.opendc.compute.simulator.price.PriceState
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask


/**
 * A [HostFilter] that filters hosts that are On-Demand.
 */
public class OnDemandInstanceFilter : HostFilter {
    override fun test(host: HostView, task: ServiceTask): Boolean {
        return host.host.getPriceState() == PriceState.ON_DEMAND
    }

    override fun toString(): String = "OnDemandFilter"


}
