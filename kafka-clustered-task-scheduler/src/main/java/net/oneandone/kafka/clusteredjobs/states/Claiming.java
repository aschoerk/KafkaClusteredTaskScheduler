package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.CLAIMED;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMED_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.ERROR;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class Claiming extends StateHandlerBase {
    public Claiming(NodeImpl node) {
        super(node, StateEnum.CLAIMING);
    }

    @Override
    protected void handleSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                claiming(task, s);
                break;
            case CLAIMED:
                task.setLocalState(ERROR);
                break;
            case UNCLAIMED:
                break;
            default:
                super.handleSignal(task, s);
        }
    }

    @Override
    protected void handleOwnSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                if (s.getReference() == null && s.getReference() == task.getUnclaimedSignalOffset() ||
                    s.getReference() != null && s.getReference().equals(task.getUnclaimedSignalOffset())) {
                    task.setLocalState(CLAIMED_BY_NODE);
                    getNode().getSender().sendSignal(task, CLAIMED);
                    getNode().getPendingHandler().scheduleTaskHandlingOnNode(task);
                    getNode().getPendingHandler().scheduleTaskHeartbeatOnNode(task);
                }
                break;
            case UNCLAIMED:
                break;
            default:
                super.handleOwnSignal(task, s);
        }
    }
}
