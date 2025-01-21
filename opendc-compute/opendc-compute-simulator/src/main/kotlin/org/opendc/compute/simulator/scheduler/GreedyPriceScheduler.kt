package org.opendc.compute.simulator.scheduler

import org.opendc.compute.simulator.scheduler.filters.ComputeFilter
import org.opendc.compute.simulator.scheduler.filters.OnDemandInstanceFilter
import org.opendc.compute.simulator.scheduler.filters.RamFilter
import org.opendc.compute.simulator.scheduler.filters.SpotInstanceFilter
import org.opendc.compute.simulator.scheduler.filters.VCpuFilter
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

public class GreedyPriceScheduler : ComputeScheduler {
    private val hosts = mutableSetOf<HostView>()
    private val filters = mutableListOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.5))

    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun select(task: ServiceTask): HostView? {

        if (task.requiresOnDemand()) {
            // only use on demand hosts
            filters.add(OnDemandInstanceFilter())
        }else{
            // only use spot hosts
            filters.add(SpotInstanceFilter())
        }

        var selectedHost = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }
            .minByOrNull { it.price }

        if (selectedHost == null && task.requiresOnDemand()) {
            filters.remove(OnDemandInstanceFilter())
            selectedHost = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }
                .minByOrNull { it.onDemandPrice }
        }

        // Remove the OnDemand/SpotInstance filters that have been added
        // Otherwise they will pile up
        while (filters.size > 3)
        {
            filters.removeLast()
        }

        return selectedHost
    }

    override fun updateHost(host: HostView) {
        if(hosts.contains(host)){
            hosts.remove(host)
            hosts.add(host)
        }
    }

}
