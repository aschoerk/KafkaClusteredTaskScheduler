package net.oneandone.kafka.clusteredjobs.states;

import net.oneandone.kafka.clusteredjobs.NodeImpl;

/**
 * @author aschoerk
 */
public interface StateInterface {
    StateHandlerBase  createState(NodeImpl node);
}
