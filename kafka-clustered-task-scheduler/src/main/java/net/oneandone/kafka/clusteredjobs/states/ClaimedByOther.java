package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.api.StateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.INITIATING;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.TaskImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

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
    protected void handleSignal(final TaskImpl task, final Signal s) {
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
    protected void handleOwnSignal(final TaskImpl task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                break;
            default:
                super.handleOwnSignal(task,s);
        }
    }

    @Override
    protected void handleInternal(final TaskImpl task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING_I:
                break;
            default:
                super.handleInternal(task, s);
        }
    }
}
