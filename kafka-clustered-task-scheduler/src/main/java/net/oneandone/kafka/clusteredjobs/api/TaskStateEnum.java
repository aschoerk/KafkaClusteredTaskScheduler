package net.oneandone.kafka.clusteredjobs.api;

/**
 * The state of a Task on a node
 */
public enum TaskStateEnum {

    /**
     * not in state-machine yet, waiting for more information from other node, if there is any
     */
    NEW,
    /**
     * initiates the claiming
     */
    INITIATING,
    /**
     * sent availability for executing the task
     */
    CLAIMING,
    /**
     * there is another node responsible for executing the task
     */
    CLAIMED_BY_OTHER,
    /**
     * this node feels responsible for executing the task
     */
    CLAIMED_BY_NODE,
    /**
     * claimed and currently executing by other nodes
     */
    HANDLING_BY_OTHER,
    /**
     * claimed and currently executing by the node itself
     */
    HANDLING_BY_NODE,
    /**
     * unexpected signals arrived
     */
    ERROR 
}
