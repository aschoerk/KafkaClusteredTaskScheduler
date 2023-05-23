package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNCLAIM_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNHANDLING_I;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class HandlingByNode extends StateHandlerBase {
    /**
     * Create statemachine note for State HANDLING_BY_NODE
     * @param node the node running the statemachine
     */
    public HandlingByNode(NodeImpl node) {
        super(node, StateEnum.HANDLING_BY_NODE);
    }

    @Override
    protected void handleSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                getNode().getSender().sendSignal(task, SignalEnum.CLAIMED);
                break;
            case CLAIMED:
                error(task, s, "somebody else claims");
                break;
            default:
                super.handleSignal(task, s);
        }
    }


    @Override
    protected void handleOwnSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case HANDLING:
            case CLAIMED:
            case HEARTBEAT:
                info(task, s, "ignored");
                break;
            default:
                error(task, s, "HEARTBEAT by other");
        }
    }

    @Override
    protected void handleInternal(final Task task, final Signal s) {
        if (s.getSignal().equals(UNCLAIM_I)) {
            startUnclaiming(task);
        } else if(s.getSignal() == SignalEnum.UNHANDLING_I) {
            if(task.getDefinition().getMaxExecutionsOnNode() == null || task.getExecutionsOnNode() < task.getDefinition().getMaxExecutionsOnNode()) {
                task.setLocalState(StateEnum.CLAIMED_BY_NODE);
                getNode().getSender().sendSignal(task, SignalEnum.CLAIMED);
                getNode().getPendingHandler().scheduleTaskHandlingOnNode(task);
                getNode().getPendingHandler().scheduleTaskHeartbeatOnNode(task);
            }
            else {
                task.clearExecutionsOnNode();
                startUnclaiming(task);
            }
        }
        else {
            super.handleInternal(task, s);
        }
    }
}
