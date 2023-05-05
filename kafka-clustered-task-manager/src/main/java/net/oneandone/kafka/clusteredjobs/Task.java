package net.oneandone.kafka.clusteredjobs;

import java.time.Duration;
import java.time.Instant;

/**
 * @author aschoerk
 */
public interface Task {

    /**
     * The initiation time
     * @return the initiation time of the task in milliseconds epochtime.
     */
    Instant getInitialTimestamp();

    /**
     * The delay after the initial setup of the task to wait before first start.
     *
     * @return the initial delay in milliseconds
     */
    Duration getInitialDelay();

    /**
     * The last timestamp, the task was started. if state is {@link SignalEnum#HANDLING} it is the startup of the
     * currently running task.
     * @return the last startup in milliseconds epoch time
     */
    Instant getLastStartup();

    /**
     * The period of the repetition of the task
     *
     * @return the period in milliseconds
     */
    Duration getPeriod();

    long maxExecutionsOnNode();

    String getName();

    TaskStateEnum getLocalState();

    Instant getHandlingStarted();

    void setLocalState(TaskStateEnum state);

    Instant getLastClaimedInfo();

    void sawClaimedInfo();

    Runnable getJob();

    long getExecutionsOnNode();

    Duration getClaimedSignalPeriod();

    Node getNode();

}
