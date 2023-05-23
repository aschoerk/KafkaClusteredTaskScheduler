package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.CLAIMING_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNCLAIMED;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMING;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class UnClaiming extends StateHandlerBase {
    /**
     * Create statemachine note for State UNCLAIMING
     * @param node the node running the statemachine
     */
    public UnClaiming(NodeImpl node) {
        super(node, StateEnum.UNCLAIMING);
    }

    @Override
    protected void handleSignal(final Task task, final Signal s) {
        info(task, s, "Unhandled own message in unclaiming");
    }

    @Override
    protected void handleOwnSignal(final Task task, final Signal s) {
        if(s.getSignal() == UNCLAIMED) {
            task.setUnclaimedSignalOffset(s.getCurrentOffset().get());
            task.setLocalState(StateEnum.INITIATING);
            getNode().getPendingHandler().scheduleTaskForClaiming(task);
        } else {
            info(task, s, "Unhandled own message in unclaiming");
        }
    }

    @Override
    protected void handleInternal(final Task task, final Signal s) {
        if(s.getSignal() == CLAIMING_I) {
            task.setLocalState(CLAIMING);
            getNode().getSender().sendSignal(task, SignalEnum.CLAIMING, task.getUnclaimedSignalOffset());
        } else {
            super.handleInternal(task, s);
        }
    }
}
