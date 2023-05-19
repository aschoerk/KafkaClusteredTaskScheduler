package net.oneandone.kafka.clusteredjobs.api;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * @author aschoerk
 */
public interface NodeTaskInformation {
    /**
     * the unique name of the node where the tasks are registered
     * @return the unique name of the node where the tasks are registered
     */
    String getName();

    /**
     * if set, the offset of the kafka-event
     * @return if set, the offset of the kafka-event
     */
    Optional<Long> getOffset();

    /**
     * if set, the time when the event containing NodeTaskInformation arrived in consumer
     * @return if set, the time when the event containing NodeTaskInformation arrived in consumer
     */
    Optional<Instant> getArrivalTime();

    /**
     * Task-Runtime-Information
     */
    interface TaskInformation {
        /**
         * the name of the task
         * @return the name of the task
         */
        String getTaskName();

        /**
         * the state of the task on the node which sent the information
         * @return the state of the task on the node which sent the information
         */
        TaskStateEnum getState();

        /**
         * in case of CLAIMED_BY_OTHER_NODE, HANDLED_BY_OTHER_NODE, the node where the node presumes the task to be handled.
         * @return in case of CLAIMED_BY_OTHER_NODE, HANDLED_BY_OTHER_NODE, the node where the node presumes the task to be handled.
         */
        Optional<String> getNodeName();
    }

    /**
     * describe all registered Tasks
     * @return the runtimeinformation of all registered tasks.
     */
    List<TaskInformation> getTaskInformation();
}
