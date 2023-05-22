package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.states.StateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.INITIATING;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class HandlingByOther extends StateHandlerBase {
    public HandlingByOther(NodeImpl node) {
        super(node, StateEnum.HANDLING_BY_OTHER);
    }

    @Override
    protected void handleSignal(final Task task, final Signal s) {
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
        }
    }

    @Override
    protected void handleOwnSignal(final Task task, final Signal s) {
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
    protected void handleInternal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case INITIATING_I:
                task.setLocalState(INITIATING);
                break;
            case CLAIMING_I:
                break;
            default:
                super.handleInternal(task, s);
        }
    }

}
