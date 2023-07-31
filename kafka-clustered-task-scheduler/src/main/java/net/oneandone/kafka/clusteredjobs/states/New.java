package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.api.StateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.INITIATING;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.NEW;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.TaskImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * handler for the new-State. A Node/Task gets initially created in this state after starting the node or creating the task.
 */
public class New extends StateHandlerBase {
    /**
     * Create statemachine note for State NEW.
     *
     * @param node the node running the statemachine
     */
    public New(final NodeImpl node) {
        super(node, StateEnum.NEW);
    }

    @Override
    protected void handleSignal(final TaskImpl task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
            case CLAIMED:
                task.setLocalState(StateEnum.CLAIMED_BY_OTHER, s);
                break;
            case UNCLAIMED:
                super.unclaimed(task, s);
                break;
            case HEARTBEAT:
                task.setLocalState(StateEnum.CLAIMED_BY_OTHER, s);
                break;
            case HANDLING:
                task.setLocalState(HANDLING_BY_OTHER, s);
                break;
            default:
                info(task, s, "ignored");
        }
    }

    @Override
    protected void handleInternal(final TaskImpl task, final Signal s) {

        if(Objects.requireNonNull(s.getSignal()) == SignalEnum.INITIATING_I) {
            if(getNode().getNodeTaskInformationHandler() != null) {
                Optional<Pair<String, SignalEnum>> lastInformation = getNode().getNodeTaskInformationHandler().getUnknownTaskSignal(task.getDefinition().getName());
                if(lastInformation.isPresent()) {
                    final Pair<String, SignalEnum> stringSignalEnumPair = lastInformation.get();
                    logger.info("Found information for new task {} INITIATING_I signal: {} node: {} ",
                            task, stringSignalEnumPair.getRight(), stringSignalEnumPair.getLeft());
                    switch (stringSignalEnumPair.getRight()) {
                        case CLAIMED:
                        case HEARTBEAT:
                            task.setLocalState(CLAIMED_BY_OTHER, stringSignalEnumPair.getLeft());
                            break;
                        case HANDLING:
                            task.setLocalState(HANDLING_BY_OTHER, stringSignalEnumPair.getLeft());
                            break;
                        default:
                            task.setLocalState(INITIATING);
                    }
                }
                else {
                    task.setLocalState(NEW);
                }
            }
            else {
                task.setLocalState(NEW);
            }
        }
        else {
            super.handleInternal(task, s);
        }
    }

}
