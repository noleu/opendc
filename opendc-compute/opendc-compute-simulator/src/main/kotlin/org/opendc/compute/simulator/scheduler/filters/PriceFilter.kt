package org.opendc.compute.simulator.scheduler.filters

import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

public class PriceFilter (private val priceThreshold: Double) : HostFilter{
    override fun test(host: HostView, task: ServiceTask): Boolean {
        return host.price <= priceThreshold
    }
}
