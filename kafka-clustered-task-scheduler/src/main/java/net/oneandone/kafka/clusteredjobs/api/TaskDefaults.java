package net.oneandone.kafka.clusteredjobs.api;

import java.time.Duration;
import java.time.Instant;

/**
 * @author aschoerk
 */
public abstract class TaskDefaults implements TaskDefinition {

    private Instant initialTimestamp = Instant.ofEpochMilli(1682074899000L);
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
        }
        return resurrectionInterval;
    }


    public static class TaskDefaultsBuilder {
        private Instant initialTimestamp;
        private Duration maxDuration;
        private int maximumUncompletedExecutionsOnNode;
        private Duration claimedSignalPeriod;
        private Long maxExecutionsOnNode;
        private Duration resurrectionInterval;

        protected TaskDefaultsBuilder() {}


        public TaskDefaultsBuilder withInitialTimestamp(Instant initialTimestamp) {
            this.initialTimestamp = initialTimestamp;
            return this;
        }

        public TaskDefaultsBuilder withMaxDuration(Duration maxDuration) {
            this.maxDuration = maxDuration;
            return this;
        }

        public TaskDefaultsBuilder withMaximumUncompletedExecutionsOnNode(int maximumUncompletedExecutionsOnNode) {
            this.maximumUncompletedExecutionsOnNode = maximumUncompletedExecutionsOnNode;
            return this;
        }

        public TaskDefaultsBuilder withClaimedSignalPeriod(Duration claimedSignalPeriod) {
            this.claimedSignalPeriod = claimedSignalPeriod;
            return this;
        }

        public TaskDefaultsBuilder withMaxExecutionsOnNode(Long maxExecutionsOnNode) {
            this.maxExecutionsOnNode = maxExecutionsOnNode;
            return this;
        }

        public TaskDefaultsBuilder withResurrectionInterval(Duration resurrectionInterval) {
            this.resurrectionInterval = resurrectionInterval;
            return this;
        }

        public TaskDefaults build(TaskDefaults taskDefaults) {
            taskDefaults.initialTimestamp = this.initialTimestamp;
            taskDefaults.maximumUncompletedExecutionsOnNode = this.maximumUncompletedExecutionsOnNode;
            taskDefaults.resurrectionInterval = this.resurrectionInterval;
            taskDefaults.maxExecutionsOnNode = this.maxExecutionsOnNode;
            taskDefaults.maxDuration = this.maxDuration;
            taskDefaults.claimedSignalPeriod = this.claimedSignalPeriod;
            return taskDefaults;
        }
    }
}
