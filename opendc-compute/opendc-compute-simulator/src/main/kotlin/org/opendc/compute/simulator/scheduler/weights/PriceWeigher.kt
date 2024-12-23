package org.opendc.compute.simulator.scheduler.weights

import org.opendc.compute.simulator.service.HostView
import org.opendc.compute.simulator.service.ServiceTask

// TODO: Verify multiplexer logic
/**
 * A [HostWeigher] that assigns a weight to a host based on its price.
 *
 * @param multiplier The multiplier to apply to the price.  A positive value will result in the scheduler preferring hosts with higher
 * price, and a negative number will result in the scheduler preferring hosts with a lower price.
 */
public class PriceWeigher(override val multiplier: Double = 1.0) : HostWeigher{
    override fun getWeight(host: HostView, task: ServiceTask): Double {
        return host.host.getCurrentPrice();
    }

    override fun toString(): String = javaClass.name;
}
