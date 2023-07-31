package net.oneandone.kafka.clusteredjobs.states;

import net.oneandone.kafka.clusteredjobs.KctmException;
import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * creates objects capable to handle signals arriving for tasks in a specific state
 */
public class StateHandlerFactory {

    /**
     * create a handler for a specific task-state
     * @param node the node for which the handler-singleton is to be created
     * @param state the task state the specific object should handle
     * @return the handler.
     */
    public StateHandlerBase createStateHandler(NodeImpl node, StateEnum state) {
        switch(state) {
            case NEW:
                return new New(node);
            case CLAIMING:
                return new Claiming(node);
            case INITIATING:
                return new Initiating(node);
            case CLAIMED_BY_NODE:
                return new ClaimedByNode(node);
            case HANDLING_BY_NODE:
                return new HandlingByNode(node);
            case CLAIMED_BY_OTHER:
                return new ClaimedByOther(node);
            case HANDLING_BY_OTHER:
                return new HandlingByOther(node);
            case UNCLAIMING:
                return new UnClaiming(node);
            case ERROR:
                return new Error(node);
            default:
                throw new KctmException("expected state factory to be able to create state handler for " + state);

        }
    }
}
