package net.oneandone.kafka.clusteredjobs.api;

/**
 * @author aschoerk
 */
public interface ClusterTask {

    /**
     * called as soon as the task is claimed at a node
     */
    default void startup() {}

    /**
     * called when the task is handled at a node
     */
    void call();

    /**
     * called when a task is unclaimed from a node.
     */
    default void shutdown() {}
}
