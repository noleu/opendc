package org.opendc.compute.simulator.scheduler

import org.opendc.compute.simulator.scheduler.filters.ComputeFilter
import org.opendc.compute.simulator.scheduler.filters.HostFilter
import org.opendc.compute.simulator.scheduler.filters.RamFilter
import org.opendc.compute.simulator.scheduler.filters.VCpuFilter
import org.opendc.compute.simulator.scheduler.filters.OnDemandInstanceFilter
import org.opendc.compute.simulator.scheduler.filters.SpotInstanceFilter
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

public class UniformProgressionScheduler : ComputeScheduler{

    private val hosts = mutableListOf<HostView>()
    private val filters = mutableListOf<HostFilter>(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.5))

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
        }
        if (task.requiresSpot()) {
            filters.add(SpotInstanceFilter())
        }

        val host = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }
            .minByOrNull { it.price }
        return host
    }
//
//    fun reSelect(task: ServiceTask): HostView? {
//        if (task.requiresOnDemand()) {
//            // only use on demand hosts
//
//        }
//
//        val host = hosts.filter { host -> host.isOnline() && host.isSuitable(task) && !isSafetyNetRuleViolated(task) }
//            .minByOrNull { it.getSpotPrice() }
//
//        return host
//    }
//
//    private fun isSafetyNetRuleViolated(task: ServiceTask): Boolean {
//
//        if (task.createdAt == null ) {
//            return false
//        }
//        if (task.deadline == null) {
//            return true
//        }
//        var computationTime = Instant.now().epochSecond - (task.launchedAt?.epochSecond ?: Instant.now().epochSecond)
//        var remainingTime = task.deadline?.epochSecond - Instant.now().epochSecond
//        return remainingTime < computationTime + 2 * task.delay
//    }
}
