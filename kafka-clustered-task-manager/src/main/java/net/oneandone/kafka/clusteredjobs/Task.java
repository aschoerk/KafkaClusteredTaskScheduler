package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;

/**
 * @author aschoerk
 */
public interface Task {

    /**
     * The initiation time
     * @return the initiation time of the task in milliseconds epochtime.
     */
    long getInitialTimestamp();

    /**
     * The delay after the initial setup of the task to wait before first start.
     * @return the initial delay in milliseconds
     */
    long getInitialDelay();

    /**
     * The last timestamp, the task was started. if state is {@link TaskSignalEnum#HANDLED_BY_ME} it is the startup of the
     * currently running task.
     * @return the last startup in milliseconds epoch time
     */
    Instant getLastStartup();

    /**
     * The period of the repetition of the task
     * @return the period in milliseconds
     */
    long getPeriod();

    String getName();

    TaskStateEnum getLocalState();

    Instant getClaimingSet();

    Instant getClaimingTimestamp();

    void setClaimingTimestamp(Instant claimingTimestampP);

    Instant getHandlingStarted();

    void setLocalState(TaskStateEnum state);

    Instant getLastClaimedInfo();

    void sawClaimedInfo();

    void unclaim();

    Runnable getJob();
}
