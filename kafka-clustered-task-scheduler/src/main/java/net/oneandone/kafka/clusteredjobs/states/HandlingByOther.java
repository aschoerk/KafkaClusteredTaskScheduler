package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.api.StateEnum.HANDLING_BY_OTHER;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.TaskImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * @author aschoerk
 */
public class HandlingByOther extends StateHandlerBase {
    /**
     * Create statemachine note for State HANDLING_BY_OTHER
     * @param node the node running the statemachine
     */
    public HandlingByOther(NodeImpl node) {
        super(node, StateEnum.HANDLING_BY_OTHER);
    }

    @Override
    protected void handleSignal(final TaskImpl task, final Signal s) {
        switch (s.getSignal()) {
            case UNCLAIMED:
                super.unclaimed(task, s);
                break;
            case HEARTBEAT:
            case CLAIMED:
                claimed(task, s);
                break;
            case HANDLING:
                task.setLocalState(HANDLING_BY_OTHER, s);
                break;
            default:
                info(task, s, "ignored signal");
        }
    }

    @Override
    protected void handleOwnSignal(final TaskImpl task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                info(task, s, "handleOwnSignal ignored");
                break;
            case CLAIMED:
            case HANDLING:
            case HEARTBEAT:
                error(task, s, "handleOwnSignal not ignored");
                break;
            default:
                super.handleOwnSignal(task, s);
        }
    }

    @Override
    protected void handleInternal(final TaskImpl task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING_I:
                break;
            case REVIVING:
                doUnclaiming(task);
                break;
            default:
                super.handleInternal(task, s);
        }
    }

}
