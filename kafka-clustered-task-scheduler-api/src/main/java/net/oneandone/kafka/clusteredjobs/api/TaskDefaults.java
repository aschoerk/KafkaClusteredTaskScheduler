package net.oneandone.kafka.clusteredjobs.api;

import java.time.Duration;
import java.time.Instant;

/**
 * @author aschoerk
 */
public abstract class TaskDefaults implements TaskDefinition {

    private Instant initialTimestamp = null;
    private Duration maxDuration = null;
    private int maximumUncompletedExecutionsOnNode = 2;
    private Duration claimedSignalPeriod = null;
    private Long maxExecutionsOnNode = null;
    private Duration resurrectionInterval = null;


    /**
     * initiate Taskstart-Modus as fixed Period from a Point in time
     * @return arbitrary point in time to make sure the default modus is fixed periods.
     */
    @Override
    public Instant getInitialTimestamp() {
        return initialTimestamp;  // Friday, April 21, 2023 11:01:39
    }

    @Override
    public Duration getMaxDuration() {
        if (maxDuration == null) {
            maxDuration = getPeriod().multipliedBy(2);
        }
        return maxDuration;
    }

    /**
     * after maxDuration a task may get restarted on a node. Theoretically tasks can run in parallel.
     * MaximumUncompletedExecutionsOnNode restricts the number of parallel startings of a task.
     * @return the number of parallel startings of a task.
     */
    @Override
    public int getMaximumUncompletedExecutionsOnNode() {
        return maximumUncompletedExecutionsOnNode;
    }

    @Override
    public Duration getClaimedSignalPeriod() {
        if (claimedSignalPeriod == null) {
            claimedSignalPeriod = getMaxDuration().multipliedBy(3);
        }
        return claimedSignalPeriod;
    }

    /**
     * default: don't restrict number of executions on a single node
     * @return don't restrict the number of executions on a single node
     */
    @Override
    public Long getMaxExecutionsOnNode() {
        return maxExecutionsOnNode;
    }

    /**
     * default: wait 3 times the resurrection-interval before starting the claim-interaction
     * @return wait 3 times the resurrection-interval before starting the claim-interaction
     */
    @Override
    public Duration getResurrectionInterval() {
        if (resurrectionInterval == null) {
            resurrectionInterval = getClaimedSignalPeriod().multipliedBy(3);
            if (resurrectionInterval.compareTo(getMaxDuration()) < 0) {
                resurrectionInterval = getMaxDuration().multipliedBy(2);
            }
        }
        return resurrectionInterval;
    }

    /**
     * set initialTimestamp
     * @param initialTimestamp the timestamp to use for calculation of the scheduling
     */
    public void setInitialTimestamp(final Instant initialTimestamp) {
        this.initialTimestamp = initialTimestamp;
    }

    /**
     * set the maximum duration of a task execution
     * @param maxDuration the maximum duration of the execution of task. After that time, the system will try to end it.
     */
    public void setMaxDuration(final Duration maxDuration) {
        this.maxDuration = maxDuration;
    }

    /**
     * after maxDuration a task may get restarted on a node. Theoretically tasks can run in parallel.
     * MaximumUncompletedExecutionsOnNode restricts the number of parallel startings of a task.
     * @param maximumUncompletedExecutionsOnNode the maximum number of parallel startings of a task.
     */
    public void setMaximumUncompletedExecutionsOnNode(final int maximumUncompletedExecutionsOnNode) {
        this.maximumUncompletedExecutionsOnNode = maximumUncompletedExecutionsOnNode;
    }

    /**
     * set the timeinterval between the sending of claimed Signals while the task is in state CLAIMED_BY_NODE
     * @param claimedSignalPeriod the timeinterval between the sending of claimed Signals while the task is in state CLAIMED_BY_NODE
     */
    public void setClaimedSignalPeriod(final Duration claimedSignalPeriod) {
        this.claimedSignalPeriod = claimedSignalPeriod;
    }

    /**
     * set the maximum number of execution on a specific node after which the task is to be unclaimed
     * @param maxExecutionsOnNode the maximum number of execution on a specific node after which the task is to be unclaimed
     */
    public void setMaxExecutionsOnNode(final Long maxExecutionsOnNode) {
        this.maxExecutionsOnNode = maxExecutionsOnNode;
    }

    /**
     * set the time interval after the last claimedSignal to wait before restarting a task
     * @param resurrectionInterval the time interval after the last claimedSignal to wait before restarting a task
     */
    public void setResurrectionInterval(final Duration resurrectionInterval) {
        this.resurrectionInterval = resurrectionInterval;
    }
}
