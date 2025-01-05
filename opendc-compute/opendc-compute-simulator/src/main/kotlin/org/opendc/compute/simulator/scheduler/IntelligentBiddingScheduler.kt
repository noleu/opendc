package org.opendc.compute.simulator.scheduler

import org.opendc.compute.simulator.price.PriceState
import org.opendc.compute.simulator.scheduler.filters.ComputeFilter
import org.opendc.compute.simulator.scheduler.filters.OnDemandInstanceFilter
import org.opendc.compute.simulator.scheduler.filters.RamFilter
import org.opendc.compute.simulator.scheduler.filters.SpotInstanceFilter
import org.opendc.compute.simulator.scheduler.filters.VCpuFilter
import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask
import kotlin.math.exp

public class IntelligentBiddingScheduler(private val reschedulePenalty: Double = 0.05) : ComputeScheduler{

    private val hosts = mutableListOf<HostView>()
    private val filters = mutableListOf(ComputeFilter(), VCpuFilter(1.0), RamFilter(1.5))


    override fun addHost(host: HostView) {
        hosts.add(host)
    }

    override fun removeHost(host: HostView) {
        hosts.remove(host)
    }

    override fun select(task: ServiceTask): HostView? {

        val remainingComputationTime = task.duration - task.currentProgress
        val timeToOnDemand = task.remainingTime - remainingComputationTime

        val rescheduleTime: Long = (task.duration * reschedulePenalty).toLong()

        val onDemandPrice: Double = getLowestAvailablePrice(task, PriceState.ON_DEMAND)
        val spotPrice: Double = getLowestAvailablePrice(task, PriceState.SPOT)

        if (timeToOnDemand - rescheduleTime > 0) {
            val bid: Double = estimateBidPrice(onDemandPrice, spotPrice, timeToOnDemand.toDouble())

            if (bid >= onDemandPrice) {
                filters.add(OnDemandInstanceFilter())
            } else {
                filters.add(SpotInstanceFilter())
            }
        } else {
            filters.add(OnDemandInstanceFilter())
        }

        val host = hosts.filter { host -> filters.all { filter -> filter.test(host, task) } }
            .minByOrNull { it.host.getCurrentPrice() }
        return host
    }


    private fun getLowestAvailablePrice(task: ServiceTask, priceState: PriceState): Double {
        var lowestPrice = Double.MAX_VALUE

        for (hv in hosts) {
            val host = hv.host
            val currentPrice = host.getPrice(priceState)
            if (host.canFit(task) && currentPrice < lowestPrice) {
                lowestPrice = currentPrice
            }
        }

        return lowestPrice
    }

    private fun estimateBidPrice(onDemandPrice: Double, spotPrice: Double, timeToOnDemand: Double): Double {
        val alpha = -0.0005
        val beta = 0.9
        val gamma = alpha * timeToOnDemand

        return exp(gamma) * onDemandPrice + (1 - exp(gamma) * (beta * onDemandPrice + (1 - beta) * spotPrice))
    }

    override fun updateHost(host: HostView) {
        if(hosts.contains(host)){
            hosts.remove(host)
            hosts.add(host)
        }
    }
}
