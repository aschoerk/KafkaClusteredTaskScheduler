package net.oneandone.kafka.clusteredjobs.api;

import java.time.Instant;

/**
 * Describes a task executiing Node.
 */
public interface Node {
    /**
     * the clusterwide unique identifier of the node
     * @return the clusterwide unique identifier of the node
     */
    String getUniqueNodeId();

    /**
     * register a task to be executed on this node
     * @param taskDefinition the description how the task is to be executed
     * @return the task-object containing the definition and additional runtime-information
     */
    Task register(TaskDefinition taskDefinition);

    /**
     * find the task of a certain name previously registered at that node
     * @param taskName the name of the task being requested
     * @return the task of a certain name previously registered at that node
     */
    Task getTask(String taskName);

    /**
     * get the current time according to the clock used by node
     * @return the current time according to the clock used by node
     */
    Instant getNow();

    /**
     * returns the container specific construct, used for this node
     * @return the container specific construct, used for this node
     */
    Container getContainer();

    /**
     * generate the current information about nodes and tasks this node knows about.
     * @return the current information about nodes and tasks this node knows about.
     */
    NodeTaskInformation getNodeInformation();
}
