/*
 * Copyright (c) 2022 AtLarge Research
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

package org.opendc.compute.simulator.service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.opendc.compute.api.TaskState;
import org.opendc.compute.simulator.TaskWatcher;
import org.opendc.compute.simulator.host.SimHost;
import org.opendc.compute.simulator.price.PriceState;
import org.opendc.compute.simulator.scheduler.GreedyPriceScheduler;
import org.opendc.compute.simulator.scheduler.IntelligentBiddingScheduler;
import org.opendc.compute.simulator.scheduler.UniformProgressionScheduler;
import org.opendc.simulator.compute.workload.Workload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ServiceTask} provided by {@link ComputeService}.
 */
public class ServiceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceTask.class);

    private final ComputeService service;
    private final UUID uid;

    private final String name;
    private final ServiceFlavor flavor;
    public Workload workload;

    private Map<String, ?> meta; // TODO: remove this

    private final List<TaskWatcher> watchers = new ArrayList<>();
    private TaskState state = TaskState.CREATED;
    Instant launchedAt = null;
    Instant createdAt;
    Instant finishedAt;
    long currentProgress = 0L;
    SimHost host = null;
    private long duration = 0L; // TODO: May be instant
    Instant deadline;
    private ComputeService.SchedulingRequest request = null;

    private int numFailures = 0;
    private boolean requiresOnDemand = false;
    private boolean requiresSpot = false;
    long lastCheckPoint;
    private long remainingTime = 0L;
    private ComputeService.ComputeClient computeClient;

    ServiceTask(
        ComputeService service,
        UUID uid,
        String name,
        ServiceFlavor flavor,
        Workload workload,
        Map<String, ?> meta) {
        this.service = service;
        this.uid = uid;
        this.name = name;
        this.flavor = flavor;
        this.workload = workload;
        this.meta = meta;
        this.duration = (long) meta.get("duration");
        this.deadline = (Instant) meta.get("deadline");

        this.createdAt = this.service.getClock().instant();
        this.computeClient = service.newClient();
    }

    @NotNull
    public UUID getUid() {
        return uid;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public ServiceFlavor getFlavor() {
        return flavor;
    }

    @NotNull
    public Map<String, Object> getMeta() {
        return Collections.unmodifiableMap(meta);
    }

    public void setWorkload(Workload newWorkload) {
        this.workload = newWorkload;
    }

    @NotNull
    public TaskState getState() {
        return state;
    }

    @Nullable
    public Instant getLaunchedAt() {
        return launchedAt;
    }

    @Nullable
    public Instant getCreatedAt() {
        return createdAt;
    }

    @Nullable
    public Instant getFinishedAt() {
        return finishedAt;
    }

    /**
     * Return the {@link SimHost} on which the task is running or <code>null</code> if it is not running on a host.
     */
    public SimHost getHost() {
        return host;
    }

    public void setHost(SimHost host) {
        this.host = host;
    }

    public int getNumFailures() {
        return this.numFailures;
    }

    public void start() {
        switch (state) {
            case PROVISIONING:
                LOGGER.debug("User tried to start task but request is already pending: doing nothing");
            case RUNNING:
                LOGGER.debug("User tried to start task but task is already running");
                break;
            case COMPLETED:
            case TERMINATED:
                LOGGER.warn("User tried to start deleted task");
                throw new IllegalStateException("Task is deleted");
            case CREATED:
                LOGGER.info("User requested to start task {}", uid);
                setState(TaskState.PROVISIONING);
                assert request == null : "Scheduling request already active";
                request = service.schedule(this);
                break;
            case FAILED:
                LOGGER.info("User requested to start task after failure {}", uid);
                setState(TaskState.PROVISIONING);
                request = service.schedule(this);
                break;
            case KICKED:
                LOGGER.info("User requested to start task after it was kicked {}", uid);
                setState(TaskState.PROVISIONING);
                request = service.schedule(this);
                break;
        }
    }

    public void watch(@NotNull TaskWatcher watcher) {
        watchers.add(watcher);
    }

    public void unwatch(@NotNull TaskWatcher watcher) {
        watchers.remove(watcher);
    }

    public void delete() {
        cancelProvisioningRequest();
        final SimHost host = this.host;
        if (host != null) {
            host.delete(this);
        }
        service.delete(this);

        this.setState(TaskState.DELETED);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceTask task = (ServiceTask) o;
        return service.equals(task.service) && uid.equals(task.uid);
    }

    public int hashCode() {
        return Objects.hash(service, uid);
    }

    public String toString() {
        return "Task[uid=" + uid + ",name=" + name + ",state=" + state + "]";
    }

    void setState(TaskState newState) {
        if (this.launchedAt != null) {
            long timeSinceLaunch = this.service.getClock().instant().minus(this.launchedAt.toEpochMilli(), ChronoUnit.MILLIS).toEpochMilli();
//            this.currentProgress = this.duration.toEpochMilli() - timeSinceLaunch;
            this.currentProgress = this.duration - timeSinceLaunch;
        }
        if (this.state == newState) {
            return;
        }

        for (TaskWatcher watcher : watchers) {
            watcher.onStateChanged(this, newState);

        }
        if (newState == TaskState.FAILED) {
            this.numFailures++;
        }

        if ((newState == TaskState.COMPLETED) || newState == TaskState.FAILED) {
            this.finishedAt = this.service.getClock().instant();

        }

        this.state = newState;
    }

    /**
     * Cancel the provisioning request if active.
     */
    private void cancelProvisioningRequest() {
        final ComputeService.SchedulingRequest request = this.request;
        if (request != null) {
            this.request = null;
            request.isCancelled = true;
        }
    }

    public boolean requiresOnDemand() {
        return requiresOnDemand;
    }

    public void requiresOnDemand(boolean requiresOnDemand) {
        this.requiresOnDemand = requiresOnDemand;
    }

    public boolean requiresSpot() {
        return requiresSpot;
    }

    public void requiresSpot(boolean requiresSpot) {
        this.requiresSpot = requiresSpot;
    }

    public long getCurrentProgress() {
        long progress = workload.getCurrentProgress();
//        LOGGER.warn("Current Progress: {}", progress);
        return workload.getCurrentProgress();
//        return this.currentProgress;
    }

    public Instant getDeadline() {
        return this.deadline;
    }

    public PriceState getPriceState() {
        if (host == null) {
            return PriceState.UNKNOWN;
        }

        return host.getPriceState(this);
    }

    public long getDuration() {
        return duration;
    }

    public long getTimeToDeadline() {
        return deadline.toEpochMilli() - this.service.getClock().millis();
    }

    public long getRemainingComputationTime() {
        return workload.getRemainingComputationTime();
    }

    public void setRemainingTime(long remainingTime) {
        this.remainingTime = remainingTime;
    }

    public void reevaluate() {
        if (this.host == null)
        {
            return;
        }

        long delay = 0;
        if (workload != null) {
            delay = workload.getDelay();
        }

        requiresOnDemand(false);
        requiresSpot(false);

        if (service.getScheduler() instanceof UniformProgressionScheduler) {
//            if (SafetyNetRuleApplies(delay)) {
//                requiresOnDemand(true);
//                requiresSpot(false);
//            }
//
//            if (HysteriaRuleApplies(delay)) {
//                requiresOnDemand(false);
//                requiresSpot(true);
//            }
            HysteriaRuleApplies(delay);
            SafetyNetRuleApplies(delay);
        }

        if (service.getScheduler() instanceof IntelligentBiddingScheduler) {
            long timeToOnDemand = this.getTimeToDeadline() - this.getRemainingComputationTime();

            long rescheduleTime = this.workload.getDelay();

            double onDemandPrice = service.getLowestAvailablePrice(this, PriceState.ON_DEMAND);
            double spotPrice = service.getLowestAvailablePrice(this, PriceState.SPOT);

            if (timeToOnDemand - rescheduleTime > 0) {
                double bid = service.estimateBidPrice(onDemandPrice, spotPrice, (double) timeToOnDemand);

                if (bid >= onDemandPrice) {
                    this.requiresOnDemand(true);
                    this.requiresSpot(false);
                } else if (bid <= spotPrice) {
                    this.requiresOnDemand(false);
                    this.requiresSpot(true);
                } else {
                    this.requiresOnDemand(true);
                    this.requiresSpot(true);
                }
            } else {
                this.requiresOnDemand();
            }
        }

        if (service.getScheduler() instanceof GreedyPriceScheduler) {
//            if (SafetyNetRuleApplies(delay)) {
//                requiresOnDemand(true);
//                requiresSpot(false);
//            }
            SafetyNetRuleApplies(delay);
        }

        PriceState currentPriceState = getPriceState();
        if ((requiresOnDemand() && currentPriceState != PriceState.ON_DEMAND) ||
            (requiresSpot() && currentPriceState != PriceState.SPOT)) {
            if (lastCheckPoint < currentProgress - delay) {
                currentProgress = currentProgress - delay;
                lastCheckPoint = currentProgress;
            }

            Workload snapshot = host.removeTaskWithSnapshot(this);
            assert snapshot != null;
            computeClient.rescheduleTask(this, snapshot);
        }
    }

    public boolean SafetyNetRuleApplies(long delay) {
        long remainingTime = getTimeToDeadline();
        long computationTime = getRemainingComputationTime();
        if (remainingTime < computationTime + 2 * delay) {
//            LOGGER.warn("Safety Net Rule applies for task {}", uid);
            this.requiresOnDemand(true);
            this.requiresSpot(false);
            return true;
        }
        return false;
//        return remainingTime < computationTime + 2 * delay;
    }

    public boolean HysteriaRuleApplies(long delay) {
        long remainingTime = getTimeToDeadline();
        long computationTime = getRemainingComputationTime();
        long currentProgress = getCurrentProgress();
        long expectedProgress = 0;
        if (remainingTime > 0) {
            expectedProgress = service.getClock().millis() * computationTime / remainingTime;
        }
//        LOGGER.warn("remainingTime: {}, computationTime: {}, Task Name: {}", remainingTime, computationTime, name);
//        LOGGER.warn("currentProgress: {}, expectedProgress: {}, Task Name: {}", currentProgress, expectedProgress, name);
        if (currentProgress > expectedProgress + 2 * delay) {
//        if (currentProgress > expectedProgress - 4 * delay) {
//            LOGGER.warn("Hysteria Rule applies for task {}", uid);
            this.requiresOnDemand(false);
            this.requiresSpot(true);
            return true;

        }
        return false;
//        return currentProgress > expectedProgress + 2 * delay;
    }
}
