package net.oneandone.kafka.clusteredjobs.api;

import java.time.Duration;
import java.time.Instant;

/**
 * @author aschoerk
 */
public abstract class TaskDefaults implements TaskDefinition {

    protected Instant initialTimestamp = null;
    protected Duration maxDuration = null;
    protected int maximumUncompletedExecutionsOnNode = 2;
    protected Duration claimedSignalPeriod = null;
    protected Long maxExecutionsOnNode = null;
    protected Duration resurrectionInterval = null;

    protected TaskDefaults() {

    }

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


}
