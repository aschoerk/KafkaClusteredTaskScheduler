package net.oneandone.kafka.clusteredjobs.api;

import net.oneandone.kafka.clusteredjobs.NodeImpl;

/**
 * The state of a TaskImpl on a node
 */
public enum StateEnum {
    /**
     * the node was just started. it is not yet clear in which state the task can really start.
     */
    NEW ,
    /**
     * initiates the claiming
     */
    INITIATING,
    /**
     * after UNCLAIMED was sent, wait for the message to arrive before going into INITIATING
     */
    UNCLAIMING,
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
    ERROR ,
}
