package net.oneandone.kafka.clusteredjobs.api;

import java.time.Duration;
import java.time.Instant;

/**
 * @author aschoerk
 */
public interface TaskDefinition {
    /**
     * if != null the next starttime will always be calculated from this point in time using period
     * @return if != null the period will always be calculated from this point in time using period
     */
    Instant getInitialTimestamp();

    /**
     * The period of the repetition of concluded tasks. if InitialTimestamp != null the period is fixed calculated from the InitialTimestamp.
     * otherwise the next execution is calculated as "end of last execution" plus period
     * if the previous task is yet running, the task will not get started.
     *
     * @return the period
     */
    Duration getPeriod();

    /**
     * Maximum Duration the task is guaranteed to be executed exclusively. After that time the system will try to stop
     * the thread handling the task and assume the task execution is ended.
     * @return maximum Duration the task is guaranteed to be executed exclusively
     */
    Duration getMaxDuration();

    /**
     * The maximum number of parallel executions on a node. This can happen if this number - 1 tasks failed to complete on
     * a node and could not be stopped by other means. After that number the node will seize to start this task.
     * @return The maximum number of parallel executions on a node.
     */
    int getMaximumUncompletedExecutionsOnNode();

    /**
     * Designates by which period the NodeImpl, having claimed a task will announce that to the other nodes. This will not be
     * sent during the actual handling of a task.
     * @return by which period the NodeImpl, having claimed a task will announce that to the other nodes.
     */
    Duration getClaimedSignalPeriod();

    /**
     * the time interval the system is waiting after the last claimedSignal before restarting the task
     * @return the time interval the system is waiting after the last claimedSignal before restarting the task
     */
    Duration getResurrectionInterval();

    /**
     * if > 0: the maximum number of consecutive executions on one node. After this the node will unclaim the task.
     * @return if > 0: the maximum number of consecutive executions on one node
     */
    Long getMaxExecutionsOnNode();

    /**
     * the name of the task
     * @return the name of the task
     */
    String getName();

    /**
     * return the code to be executed
     * @param node the node-environment used to execute the code
     * @return the code to be executed
     */
    Runnable getCode(Node node);
}
