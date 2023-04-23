package net.oneandone.kafka.clusteredjobs;

/**
 * The state of a Task on a node
 */
public enum TaskStateEnum {

    INITIATING, // initiates the claiming
    CLAIMING, // sent availability for executing the task
    CLAIMED_BY_OTHER,  // there is another node responsible for executing the task
    CLAIMED_BY_NODE,  // this node feels responsible for executing the task
    HANDLING_BY_OTHER, // claimed and currently executing by other nodes
    HANDLING_BY_NODE, // claimed and currently executing by the node itself
    UNCLAIM  // not responsible for executing the task
}
