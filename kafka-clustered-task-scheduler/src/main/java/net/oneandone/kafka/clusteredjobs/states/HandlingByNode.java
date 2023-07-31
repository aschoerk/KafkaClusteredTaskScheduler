package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNCLAIM_I;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.TaskImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

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
    protected void handleSignal(final TaskImpl task, final Signal s) {
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
    protected void handleOwnSignal(final TaskImpl task, final Signal s) {
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
    protected void handleInternal(final TaskImpl task, final Signal s) {
        if (s.getSignal() == UNCLAIM_I) {
            doUnclaiming(task);
        } else if(s.getSignal() == SignalEnum.UNHANDLING_I) {
            if((task.getDefinition().getMaxExecutionsOnNode() == null) || (task.getExecutionsOnNode() < task.getDefinition().getMaxExecutionsOnNode())) {
                task.setLocalState(StateEnum.CLAIMED_BY_NODE);
                getNode().getSender().sendSignal(task, SignalEnum.CLAIMED);
                getNode().getPendingHandler().scheduleTaskHeartbeatOnNode(task);
                getNode().getPendingHandler().scheduleTaskHandlingOnNode(task);
            }
            else {
                task.clearExecutionsOnNode();
                doUnclaiming(task);
            }
        }
        else {
            super.handleInternal(task, s);
        }
    }
}
