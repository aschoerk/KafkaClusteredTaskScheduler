package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.NEW;

import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class Null extends StateHandlerBase {
    /**
     * Create statemachine note for State NULL
     * @param node the node running the statemachine
     */
    public Null(NodeImpl node) {
        super(node, StateEnum.NULL);
    }

    @Override
    protected void handleInternal(final Task task, final Signal s) {

        switch (s.getSignal()) {
            case INITIATING_I:
                if (getNode().getNodeTaskInformationHandler() != null) {
                    Optional<Pair<String, SignalEnum>> lastInformation = getNode().getNodeTaskInformationHandler().getUnknownTaskSignal(task.getDefinition().getName());
                    if(lastInformation.isPresent()) {
                        switch (lastInformation.get().getRight()) {
                            case CLAIMED:
                            case HEARTBEAT:
                                task.setLocalState(CLAIMED_BY_OTHER, lastInformation.get().getLeft());
                                break;
                            case HANDLING:
                                task.setLocalState(HANDLING_BY_OTHER, lastInformation.get().getLeft());
                                break;
                            default:
                                task.setLocalState(NEW);
                        }
                    }
                    else {
                        task.setLocalState(NEW);
                    }
                } else {
                    task.setLocalState(NEW);
                }
                break;
            default:
                super.handleInternal(task, s);
        }
    }
}
