package net.oneandone.kafka.clusteredjobs.states;

import net.oneandone.kafka.clusteredjobs.NodeImpl;

/**
 * @author aschoerk
 */
public interface StateInterface {
    /**
     * create the Statemachine node for a certain StateEnum-Value
     * @param node the node running the statemachine
     * @return the Statemachine node capable to handle signals
     */
    StateHandlerBase  createState(NodeImpl node);
}
