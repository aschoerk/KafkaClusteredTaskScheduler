package net.oneandone.kafka.clusteredjobs.states;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class Error extends StateHandlerBase {
    /**
     * Create statemachine note for State ERROR
     * @param node the node running the statemachine
     */
    public Error(NodeImpl node) {
        super(node, StateEnum.ERROR);
    }
    @Override
    protected void handleSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case UNCLAIMED:
                super.unclaimed(task,s);
                break;
            default:
                super.handleSignal(task, s);
        }
    }
}
