package net.oneandone.kafka.clusteredjobs.states;

import java.util.Objects;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.TaskImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

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
    protected void handleSignal(final TaskImpl task, final Signal s) {
        if(Objects.requireNonNull(s.getSignal()) == SignalEnum.UNCLAIMED) {
            super.unclaimed(task, s);
        }
        else {
            super.handleSignal(task, s);
        }
    }
}
