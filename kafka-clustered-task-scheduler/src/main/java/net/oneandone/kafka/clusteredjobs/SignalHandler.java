package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.DOHEARTBEAT;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.HANDLING;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.HEARTBEAT;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNHANDLING_I;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.CLAIMED_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.CLAIMING;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.HANDLING_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.INITIATING;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.NEW;

import java.time.Duration;
import java.util.Map;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.TaskStateEnum;

/**
 * @author aschoerk
 */
public class SignalHandler {
    Logger logger = LoggerFactory.getLogger(SignalHandler.class);

    private final NodeImpl node;

    private boolean startingUp = true;
    private SignalsWatcher watcher;

    public SignalHandler(NodeImpl node) {
        this.node = node;
    }

    long handlerThreadCounter;

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
                        task.setLastSignal(s);
                        s.signal.handle(this, task, s);
                    }
                });
    }

    public void handleInternal(final Task task, final SignalEnum s) {
        logger.info("handle Task {} State {} signal {}", task.getDefinition().getName(), task.getLocalState(), s);
        if(!s.isInternal()) {
            throw new KctmException("Expected internal Signal");
        }
        s.handle(this, task, new InternalSignal(task, s));
    }

    void resurrecting(final Task task, final Signal s) {
        if(task.getLocalState() == CLAIMED_BY_OTHER || task.getLocalState() == HANDLING_BY_OTHER) {
            if(task.getLastClaimedInfo().plus(task.getDefinition().getResurrectionInterval()).isBefore(node.getNow())) {
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


    void unclaimedEvent(final Task task, final Signal s) {
        if(s.equalNode(node)) {
            if(task.getLocalState() != INITIATING && task.getLocalState() != CLAIMING) {
                unexpectedSignal(task, s);
            }
        }
        else {
            switch (task.getLocalState()) {
                case NEW:
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
                    unexpectedSignal(task, s);
                    break;
                case ERROR:
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

    void claimedEvent(final Task task, Signal s) {
        if(s.equalNode(task.getNode())) {
            if(task.getLocalState() != CLAIMED_BY_NODE && task.getLocalState() != HANDLING_BY_NODE) {
                unexpectedSignal(task, s);
            }
        }
        else {
            if(task.getLocalState() == NEW) {
                task.setLocalState(CLAIMED_BY_OTHER, s);
            }
            else if(task.getLocalState() == CLAIMED_BY_OTHER) {
                if(!s.nodeProcThreadId.equals(task.getCurrentExecutor().get())) {
                    unexpectedSignal(task, s);
                }
            }
            else {
                unexpectedSignal(task, s);
            }
        }
    }

    void claimingEvent(final Task task, final Signal s) {
        switch (task.getLocalState()) {
            case NEW:
                if(s.equalNode(node)) {
                    unexpectedSignal(task,s);
                } else {
                    task.setLocalState(CLAIMED_BY_OTHER, s);
                }
                break;
            case CLAIMING:
                if(s.equalNode(node)) {
                    // node.getSender().sendSynchronous(task, SignalEnum.CLAIMED);
                    task.setLocalState(CLAIMED_BY_NODE);
                    node.getPendingHandler().scheduleTaskHandlingOnNode(task);
                    node.getPendingHandler().scheduleTaskHeartbeatOnNode(task);
                }
                else {
                    task.setLocalState(CLAIMED_BY_OTHER, s);
                }
                break;
            case INITIATING:
                if(s.equalNode(node)) {
                    unexpectedSignal(task, s);
                    break;
                }
                else {
                    task.setLocalState(CLAIMED_BY_OTHER, s);
                }
                break;
            case CLAIMED_BY_OTHER:
            case HANDLING_BY_OTHER:
                // previous CLAIMING and HANDLING by other already arrived
                break;
            case CLAIMED_BY_NODE:
            case HANDLING_BY_NODE:
                if(s.equalNode(node)) {
                    // this event should already have arrived, otherwise I would yet be CLAIMING
                    unexpectedSignal(task, s);
                }
                else {
                    node.getSender().sendSynchronous(task, SignalEnum.CLAIMED);
                }
                break;
            case ERROR:
            default:
                throw new KctmException("Unexpected State: " + task.getLocalState());
        }

    }

    public void new_i(final Task task, final Signal s) {
        Signal lastSignal = task.getLastSignal();
        if(lastSignal.signal == DOHEARTBEAT) {
            task.setLocalState(INITIATING);
            node.getPendingHandler().scheduleTaskForClaiming(task);
        }
    }


    void initiating_i(final Task task, final Signal s) {
        if (task.getLocalState() == null) {
            task.setLocalState(NEW);
            node.tasks.put(task.getDefinition().getName(), task);
            node.getSender().sendSynchronous(task, DOHEARTBEAT);
        } else {
            task.setLocalState(INITIATING);
            node.getPendingHandler().scheduleTaskForClaiming(task);
        }
    }


    void heartbeat_i(final Task task, final Signal s) {
        if(task.getLocalState() != TaskStateEnum.CLAIMED_BY_NODE) {
            unexpectedSignal(task, s);
        }
        else {
            node.getSender().sendSynchronous(task, SignalEnum.HEARTBEAT);
            node.getPendingHandler().scheduleTaskHeartbeatOnNode(task);
        }
    }

    void heartbeatEvent(final Task task, final Signal s) {
        if(s.equalNode(task.getNode())) {
            if(task.getLocalState() != CLAIMED_BY_NODE && task.getLocalState() != HANDLING_BY_NODE) {
                unexpectedSignal(task, s);
            }
        }
        else {
            switch (task.getLocalState()) {
                case NEW:
                case INITIATING:
                    task.setLocalState(CLAIMED_BY_OTHER, s);
                    break;
                case CLAIMED_BY_OTHER:
                    break;
                default:
                    unexpectedSignal(task, s);
            }
        }
    }


    void handling_i(final Task task, final Signal s) {
        if(task.getLocalState() != TaskStateEnum.CLAIMED_BY_NODE) {
            logger.error("Starting Task {} but not Claimed_by_N: {}", task);
        }
        else {
            final String threadName = task.getDefinition().getName() + "_" + Thread.currentThread().getId() + "_" + handlerThreadCounter++;
            task.setLocalState(TaskStateEnum.HANDLING_BY_NODE);
            Pair<SignalsWatcher, Thread> p = MutablePair.of(watcher, null);
            p.setValue(node.newHandlerThread(new Runnable() {
                @Override
                public void run() {
                    try {
                        node.getPendingHandler().removeClaimedSignaller(task); // claim could get lost when running job
                        task.getDefinition().getJob(node).run();
                    } finally {
                        node.getSignalHandler().handleInternal(task, UNHANDLING_I);
                        node.disposeHandlerThread(p.getValue());
                    }
                }
            }));
            p.getValue().setName(threadName);
            p.getValue().start();
            this.node.getPendingHandler().scheduleInterupter(task, threadName, p.getValue());
        }
    }


    void unhandling_i(final Task task, final Signal s) {
        if(task.getLocalState() == TaskStateEnum.HANDLING_BY_NODE) {
            if(task.getDefinition().getMaxExecutionsOnNode() == null || task.getExecutionsOnNode() < task.getDefinition().getMaxExecutionsOnNode()) {
                task.setLocalState(TaskStateEnum.CLAIMED_BY_NODE);
                node.getPendingHandler().scheduleTaskHandlingOnNode(task);
                node.getPendingHandler().scheduleTaskHeartbeatOnNode(task);
            }
            else {
                task.setExecutionsOnNode(0);
                handleInternal(task, SignalEnum.UNCLAIM_I);
            }
        }
        else {
            if(!startingUp) {
                unexpectedSignal(task, s);
            }
        }
    }

    void handlingEvent(final Task task, final Signal s) {
        if(s.equalNode(node)) {
            if(task.getLocalState() != HANDLING_BY_NODE) {
                unexpectedSignal(task, s);
            }
        }
        else {
            // the handling event sent during handling by self
            // was yet in queue after unclaiming
            if(task.getLocalState() == CLAIMED_BY_OTHER || task.getLocalState() == HANDLING_BY_OTHER || task.getLocalState() == NEW) {
                task.setLocalState(HANDLING_BY_OTHER, s);
            }
            else {
                unexpectedSignal(task, s);
            }
        }
    }

    private void unexpectedSignal(final Task task, final Signal s) {
        String stacked = "";
        if(task.getLocalState() == ERROR) {
            stacked = "Stacked: ";
        }
        logger.error("{}Task {} in state: {} set in Error because of unexpected Signal {}", stacked, task.getDefinition().getName(), task.getLocalState(), s);
        task.setLocalState(ERROR);
    }

    public void doheartbeat(final Task task, final Signal s) {
        if(!s.equalNode(node)) {
            node.sendHeartbeat();
        }
    }
}
