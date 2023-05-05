package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNHANDLING_I;
import static net.oneandone.kafka.clusteredjobs.TaskStateEnum.CLAIMED_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.TaskStateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.TaskStateEnum.CLAIMING;
import static net.oneandone.kafka.clusteredjobs.TaskStateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.TaskStateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.TaskStateEnum.INITIATING;

import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
public class SignalHandler {
    Logger logger = LoggerFactory.getLogger(SignalHandler.class);

    private final Node node;

    private boolean startingUp = true;
    private SignalsWatcher watcher;

    public SignalHandler(Node node) {
        this.node = node;
    }

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
                    logger.info("handle Task {} State {} signal {}/{} self: {}", task.getName(), task.getLocalState()
                            , s.signal, s.getCurrentOffset(), s.nodeProcThreadId.equals(node.getUniqueNodeId()));
                    s.signal.handle(this, task, s);
                });
    }

    public void handle(final Task task, final SignalEnum s) {
        logger.info("handle Task {} State {} signal {}", task.getName(), task.getLocalState(), s);
        if(!s.isInternal()) {
            throw new KctmException("Expected internal Signal");
        }
        s.handle(this, task, new InternalSignal(task, s));
    }

    void resurrecting(final Task task, final Signal s) {
        if(task.getLocalState() == CLAIMED_BY_OTHER || task.getLocalState() == HANDLING_BY_OTHER) {
            if(task.getLastClaimedInfo().plus(task.getClaimedSignalPeriod().multipliedBy(3)).isBefore(node.getNow())) {
                initiating_i(task, s);
            }
            else {
                node.getPendingHandler().scheduleTaskResurrection(task);
            }
        }
    }

    void unclaim_i(final Task task, final Signal s) {
        if(task.getLocalState() == TaskStateEnum.CLAIMED_BY_NODE || task.getLocalState() == TaskStateEnum.HANDLING_BY_NODE) {
            node.getPendingHandler().removeTaskStarter(task);
            node.getPendingHandler().removeClaimedSignaller(task);
            task.setLocalState(TaskStateEnum.INITIATING);
            node.getSender().sendSynchronous(task, SignalEnum.UNCLAIMED);
            node.getPendingHandler().scheduleTaskForClaiming(task);
            // this can set task to CLAIMING before UNCLAIMED arrives but for following states,
            // triggering events must happen after the UNCLAIMED for self arrived
        }
    }


    void unclaim(final Task task, final Signal s) {
        if(s.equalNode(node)) {
            if(task.getLocalState() != INITIATING && task.getLocalState() != CLAIMING) {
                unexpectedSignal(task, s);
            }
        }
        else {
            switch (task.getLocalState()) {
                case CLAIMED_BY_OTHER:
                case HANDLING_BY_OTHER:
                    task.setLocalState(INITIATING);
                    node.getPendingHandler().scheduleTaskForClaiming(task);
                    node.getPendingHandler().removeTaskResurrection(task);
                    break;
                case INITIATING:
                case CLAIMING:
                    break;
                case CLAIMED_BY_NODE:
                case HANDLING_BY_NODE:
                    logger.warn("Claiming/Handling Task {} got unclaim from foreign node {}", task, s);
                    break;
                default:
                    throw new KctmException("Unexpected State: " + task.getLocalState());
            }
        }
    }


    void claiming_i(final Task task, final Signal s) {
        if(task.getLocalState() == TaskStateEnum.INITIATING) {
            task.setLocalState(CLAIMING);
            node.getSender().sendSynchronous(task, SignalEnum.CLAIMING);
        }
        else {
            logger.trace("Too late for claiming task {}", task);
        }
    }

    void claiming(final Task task, final Signal s) {
        switch (task.getLocalState()) {
            case CLAIMING:
                if(s.equalNode(node)) {
                    node.getSender().sendSynchronous(task, SignalEnum.CLAIMED);
                    task.setLocalState(CLAIMED_BY_NODE);
                    node.getPendingHandler().scheduleTaskHandlingOnNode(task);
                    node.getPendingHandler().scheduleTaskClaimedOnNode(task);
                }
                else {
                    task.setLocalState(CLAIMED_BY_OTHER);
                }
                break;
            case INITIATING:
                if(s.equalNode(node)) {
                    unexpectedSignal(task, s);
                }
            case CLAIMED_BY_OTHER:
            case HANDLING_BY_OTHER:
                // previous CLAIMING by other already arrived
                task.setLocalState(CLAIMED_BY_OTHER);
                break;
            case CLAIMED_BY_NODE:
            case HANDLING_BY_NODE:
                if(s.equalNode(node)) {
                    // this event should already have arrived, otherwise I would yet be CLAIMING
                    unexpectedSignal(task, s);
                }
                break;
            default:
                throw new KctmException("Unexpected State: " + task.getLocalState());
        }

    }

    void initiating_i(final Task task, final Signal s) {
        task.setLocalState(TaskStateEnum.INITIATING);
        node.tasks.put(task.getName(), task);
        node.getPendingHandler().scheduleTaskForClaiming(task);
    }

    void initiating(final Task task, final Signal s) {
        switch (task.getLocalState()) {
            case HANDLING_BY_NODE:
                if(!s.equalNode(node)) {
                    node.getSender().sendSynchronous(task, SignalEnum.HANDLING);
                }
                else {
                    unexpectedSignal(task, s);
                }
                break;
            case CLAIMED_BY_NODE:
                if(s.equalNode(node)) {
                    node.getSender().sendSynchronous(task, SignalEnum.CLAIMED);
                }
                break;
            case INITIATING:
                break;
            case CLAIMED_BY_OTHER:
            case HANDLING_BY_OTHER:
            case CLAIMING:
                if(s.equalNode(node)) {
                    // should not be already handling
                    unexpectedSignal(task, s);
                }
                else {
                    task.setLocalState(CLAIMED_BY_OTHER);
                }
                break;
            default:
                throw new KctmException("Unexpected State: " + task.getLocalState());
        }
    }

    void claimed_i(final Task task, final Signal s) {
        if(task.getLocalState() != TaskStateEnum.CLAIMED_BY_NODE && task.getLocalState() != TaskStateEnum.HANDLING_BY_NODE) {
            logger.warn("Starting Task {} but not Claimed_by_N: {}", task);
        }
        else {
            node.getSender().sendSynchronous(task, task.getLocalState() == TaskStateEnum.CLAIMED_BY_NODE ? SignalEnum.CLAIMED : SignalEnum.HANDLING);
            node.getPendingHandler().scheduleTaskClaimedOnNode(task);
        }
    }

    void claimed(final Task task, final Signal s) {
        claimedOrHandling(task, s, CLAIMED_BY_OTHER);

    }


    void handling_i(final Task task, final Signal s) {
        if(task.getLocalState() != TaskStateEnum.CLAIMED_BY_NODE) {
            logger.error("Starting Task {} but not Claimed_by_N: {}", task);
        }
        else {
            task.setLocalState(TaskStateEnum.HANDLING_BY_NODE);
            Pair<SignalsWatcher, Thread> p = MutablePair.of(watcher, null);
            p.setValue(node.newHandlerThread(new Runnable() {
                @Override
                public void run() {
                    try {
                        node.getPendingHandler().removeClaimedSignaller(task);  // claim could get lost when running job
                        task.getJob().run();
                    } finally {
                        node.getSignalHandler().handle(task, UNHANDLING_I);
                        node.disposeHandlerThread(p.getValue());
                    }
                }
            }));
            p.getValue().setName(task.getName() + "_HANDLING");
            p.getValue().start();
        }
    }


    void unhandling_i(final Task task, final Signal s) {
        if(task.getLocalState() == TaskStateEnum.HANDLING_BY_NODE) {
            if(task.getExecutionsOnNode() < task.maxExecutionsOnNode()) {
                task.setLocalState(TaskStateEnum.CLAIMED_BY_NODE);
                node.getPendingHandler().scheduleTaskHandlingOnNode(task);
                node.getPendingHandler().scheduleTaskClaimedOnNode(task);
            }
            else {
                handle(task, SignalEnum.UNCLAIM_I);
            }
        }
        else {
            if(!startingUp) {
                unexpectedSignal(task, s);
            }
        }
    }

    void handling(final Task task, final Signal s) {
        if(task.getLocalState() != INITIATING || !s.equalNode(node)) {
            claimedOrHandling(task, s, HANDLING_BY_OTHER);
        }
        else {
            // the handling event sent during handling by self
            // was yet in queue after unclaiming
        }
    }

    private void claimedOrHandling(final Task task, final Signal s, final TaskStateEnum claimedOrHandlingState) {
        switch (task.getLocalState()) {
            case HANDLING_BY_NODE:
            case CLAIMED_BY_NODE:
                if(!s.equalNode(node)) {
                    unexpectedSignal(task, s);
                }
                break;
            case INITIATING:
            case CLAIMED_BY_OTHER:
            case HANDLING_BY_OTHER:
            case CLAIMING:
                if(s.equalNode(node)) {
                    // should not be already handling
                    unexpectedSignal(task, s);
                }
                else {
                    task.setLocalState(claimedOrHandlingState);
                    node.getPendingHandler().scheduleTaskResurrection(task);
                }
                break;
            default:
                throw new KctmException("Unexpected State: " + task.getLocalState());
        }
    }

    private void unexpectedSignal(final Task task, final Signal s) {
        logger.error("Task {} in state: {} set in Error because of unexpected Signal {}", task.getName(), task.getLocalState(), s);
        task.setLocalState(ERROR);
    }

}
