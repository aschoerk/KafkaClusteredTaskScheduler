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
public class Initiating extends StateHandlerBase {
    /**
     * Create statemachine note for State INITIATING
     * @param node the node running the statemachine
     */
    public Initiating(NodeImpl node) {
        super(node, StateEnum.INITIATING);
    }


    @Override
    protected void handleSignal(final TaskImpl task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                claiming(task, s);
                break;
            case HEARTBEAT:
                task.setLocalState(StateEnum.CLAIMED_BY_OTHER,s);
                break;
            case CLAIMED:
                task.setLocalState(StateEnum.ERROR);
                break;
            case UNCLAIMED:
                unclaimed(task, s);
                break;
            default:
                super.handleSignal(task, s);
        }
    }

    @Override
    protected void handleOwnSignal(final TaskImpl task, final Signal s) {
        if(Objects.requireNonNull(s.getSignal()) == SignalEnum.UNCLAIMED) {
        }
        else {
            super.handleOwnSignal(task, s);
        }
    }

    @Override
    protected void handleInternal(final TaskImpl task, final Signal s) {
        if(Objects.requireNonNull(s.getSignal()) == SignalEnum.CLAIMING_I) {
            task.setLocalState(StateEnum.CLAIMING);
            getNode().getSender().sendSignal(task, SignalEnum.CLAIMING, task.getUnclaimedSignalOffset());
        }
        else {
            super.handleInternal(task, s);
        }
    }

}
