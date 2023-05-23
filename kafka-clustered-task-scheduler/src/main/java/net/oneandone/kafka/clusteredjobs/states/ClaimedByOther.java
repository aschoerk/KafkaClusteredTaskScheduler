package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.INITIATING;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class ClaimedByOther extends StateHandlerBase {

    /**
     * Create statemachine note for State CLAIMED_BY_OTHER
     * @param node the node running the statemachine
     */
    public ClaimedByOther(NodeImpl node) {
        super(node, StateEnum.CLAIMED_BY_OTHER);
    }

    @Override
    protected void handleSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case UNCLAIMED:
                super.unclaimed(task,s);
                break;
            case CLAIMING:
                info(task, s, "old claiming arrived");
                break;
            case CLAIMED:
                super.claimed(task, s);
                break;
            case HEARTBEAT:
                break;
            case HANDLING:
                task.setLocalState(HANDLING_BY_OTHER, s);
                break;
            default:
                super.handleSignal(task, s);
        }
    }

    @Override
    protected void handleOwnSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                break;
            default:
                super.handleOwnSignal(task,s);
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
