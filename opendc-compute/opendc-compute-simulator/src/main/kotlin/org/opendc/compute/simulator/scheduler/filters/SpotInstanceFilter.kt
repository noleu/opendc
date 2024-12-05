package org.opendc.compute.simulator.scheduler.filters

import org.opendc.compute.simulator.price.PriceState
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

public class SpotInstanceFilter : HostFilter {
    override fun test(host: HostView, task: ServiceTask): Boolean {
        return host.priceState == PriceState.SPOT
    }

    override fun toString(): String = "SpotInstanceFilter"
}
