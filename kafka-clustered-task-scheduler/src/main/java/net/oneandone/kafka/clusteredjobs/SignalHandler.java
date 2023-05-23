package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.CLAIMED;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.HANDLING;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNCLAIMED;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNHANDLING_I;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMED_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMING;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.HANDLING_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.INITIATING;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.NEW;

import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.states.StateHandlerBase;
import net.oneandone.kafka.clusteredjobs.states.StateEnum;

/**
 * Handles Statechanges of the local task-state-machine.
 * Statechanges are initiated by signals internal or coming via sync-topic
 */
public class SignalHandler {
    Logger logger = LoggerFactory.getLogger(SignalHandler.class);

    private final NodeImpl node;

    private StateHandlerBase[] stateHandlers = new StateHandlerBase[StateEnum.values().length];

    SignalHandler(NodeImpl node) {
        this.node = node;
        for (StateEnum e: StateEnum.values()) {
            stateHandlers[e.ordinal()] = e.createState(node);
        }
    }

    long handlerThreadCounter;


    /**
     * handle signals arrived from sync-topic for a specific task
     * @param taskName the name of the task of all signals
     * @param signals the signals to be handled.
     */
    public synchronized void handle(String taskName, Map<String, Signal> signals) {
        final Task task = node.tasks.get(taskName);
        if(task.getLocalState() == ERROR) {
            signals.entrySet().forEach(e ->
                    logger.error("N: {} in error state received from {} signal {}", task, e.getKey(), e.getValue()));
        }
        signals.values()
                .stream()
                .sorted()
                .forEach(s -> {
                    logger.info("handle Task {} State {} signal/offset {}/{} self: {}", task.getDefinition().getName(), task.getLocalState()
                            , s.signal, s.getCurrentOffset().get(), s.nodeProcThreadId.equals(node.getUniqueNodeId()));
                    if(s.signal.isInternal() && !s.nodeProcThreadId.equals(node.getUniqueNodeId())) {
                        task.setLocalState(ERROR);
                    }
                    else {
                        stateHandlers[task.getLocalState().ordinal()].handle(task, s);
                    }
                });
    }

    /**
     * handle signals arrived from sync-topic for a specific task
     * @param task the task for which a signal is to be handled
     * @param s the signal to be handled.
     */
    public void handleSignal(final Task task, final Signal s) {
        logger.info("handle Task {} State {} signal {}", task.getDefinition().getName(), task.getLocalState(), s);
        stateHandlers[task.getLocalState().ordinal()].handle(task,  s);
    }

    /**
     * handle signals generated internally in the node
     * @param task the task the signal is created for
     * @param s the SignalEnum of the internal signal
     */
    public void handleInternalSignal(final Task task, final SignalEnum s) {
        if(!s.isInternal()) {
            throw new KctmException("Expected internal Signal");
        }
        handleSignal(task, new InternalSignal(task, s));
    }


}
