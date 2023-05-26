package net.oneandone.kafka.clusteredjobs.states;

import net.oneandone.kafka.clusteredjobs.KctmException;
import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * @author aschoerk
 */
public class StateHandlerFactory {
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
