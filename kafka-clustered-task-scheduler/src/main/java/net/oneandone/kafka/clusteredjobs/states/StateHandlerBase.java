package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.api.StateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.HANDLING_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.INITIATING;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.UNCLAIMING;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.KctmException;
import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.TaskImpl;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * @author aschoerk
 */
public class StateHandlerBase {

    static Logger logger = LoggerFactory.getLogger(StateHandlerBase.class);
    private final NodeImpl node;
    private final StateEnum state;

    StateHandlerBase(NodeImpl node, StateEnum stateEnum) {
        this.node = node;
        this.state = stateEnum;
    }

    /**
     * handle the statechange to CLAIMED_BY_OTHER cause by CLAIMING-signal
     *
     * @param task the task which is currently handled
     * @param s    the signal which is currently handled
     */
    protected static void claiming(final TaskImpl task, final Signal s) {
        if(Objects.equals(s.getReference(),task.getUnclaimedSignalOffset())) {
            task.setLocalState(CLAIMED_BY_OTHER, s);
        }
    }

    /**
     * Output information about current task, state and signal
     *
     * @param task    the task which is currently handled
     * @param s       the signal which is currently handled
     * @param message additional information concerning the current processing
     */
    protected void info(TaskImpl task, Signal s, String message) {
        logger.info("N: {} T: {}/{}  S: {}/{}/{} {}", node.getUniqueNodeId(), task.getDefinition().getName(), task.getLocalState(),
                s.getNodeProcThreadId(), s.getSignal(), s.getCurrentOffset().orElse(-1L), message);
    }

    /**
     * Output error information about current task, state and signal set task in state error
     *
     * @param task    the task which is currently handled
     * @param s       the signal which is currently handled
     * @param message additional information concerning the current processing
     */
    protected void error(TaskImpl task, Signal s, String message) {
        logger.error("N: {} T: {}/{}  S: {}/{}/{} {}", node.getUniqueNodeId(), task.getDefinition().getName(), task.getLocalState(),
                s.getNodeProcThreadId(), s.getSignal(), s.getCurrentOffset().orElse(-1L), message);
        task.setLocalState(ERROR);
    }

    /**
     * handle a signal in this state, the task is presumed to be currently in this state
     *
     * @param task the task which is currently handled
     * @param s    the signal which is currently handled
     */
    public void handle(final TaskImpl task, Signal s) {
        synchronized (task) {
            if(task.getLocalState() != state) {
                throw new KctmException("Expected states to match between statehandler " + state + " and task: " + task.getLocalState());
            }
            if(s.getSignal().isInternal()) {
                info(task, s, "internal");
                handleInternal(task, s);
            }
            else if(node.getUniqueNodeId().equals(s.getNodeProcThreadId())) {
                info(task, s, "to Me");
                handleOwnSignal(task, s);
            }
            else {
                info(task, s, "event");
                handleSignal(task, s);
            }
        }
    }

    /**
     * handle signal unclaimed from other node. make sure the offset of the signal is remembered for later.
     *
     * @param task the local task found for the signal
     * @param s    the signal coming from another node
     */
    protected void unclaimed(final TaskImpl task, Signal s) {
        task.setUnclaimedSignalOffset(s.getCurrentOffset().orElse(-1L));
        task.setLocalState(INITIATING);
        node.getPendingHandler().scheduleTaskForClaiming(task);
        node.getPendingHandler().removeTaskResurrection(task);
    }

    /**
     * handle signal coming from other node, default go in error state.
     *
     * @param task the local task found for the signal
     * @param s    the signal coming from another node
     */
    protected void handleSignal(final TaskImpl task, final Signal s) {
        error(task, s, "did not expect foreign signal in this state");
    }


    /**
     * default go in error state
     *
     * @param task the task for which a signal from this node arrived
     * @param s    the signal from this node that arrived
     */
    protected void handleOwnSignal(final TaskImpl task, final Signal s) {
        error(task, s, "did not expect this own signal in this state");
    }

    /**
     * handle internal signals. e.g. from task interrupter, CLAIMING-Timeout, ...
     *
     * @param task the task for which a signal is to be handled
     * @param s    the internal signal.
     */
    protected void handleInternal(TaskImpl task, Signal s) {
        error(task, s, "did not expect internal signal in this state");
    }


    /**
     * return the node on which the statemachine is running
     *
     * @return the node on which the statemachine is running
     */
    protected NodeImpl getNode() {
        return node;
    }

    /**
     * if getting signal that a task is claimed by other node, check th
     *
     * @param task the task for which the signal arrived
     * @param s    the signal which arrived
     */
    protected void claimed(final TaskImpl task, final Signal s) {
        final Optional<String> currentExecutor = task.getCurrentExecutor();
        if((task.getLocalState() == CLAIMED_BY_OTHER || task.getLocalState() == HANDLING_BY_OTHER)
           && currentExecutor.isPresent() && !s.getNodeProcThreadId().equals(currentExecutor.get())) {
            info(task, s, "Executor " + currentExecutor.get() + " different");
        }
        task.setLocalState(CLAIMED_BY_OTHER, s.getNodeProcThreadId());
        task.sawClaimedInfo();
    }

    /**
     * common logic to handle the starting of the state UNCLAIMING
     *
     * @param task the task for which the state is to be set to UNCLAIMING
     */
    protected void startUnclaiming(final TaskImpl task) {
        getNode().getPendingHandler().removeTaskStarter(task);
        getNode().getPendingHandler().removeClaimedHeartbeat(task);
        task.setLocalState(UNCLAIMING);
        getNode().getSender().sendSignal(task, SignalEnum.UNCLAIMED);
        // this can set task to CLAIMING before UNCLAIMED arrives but for following states,
        // triggering events must happen after the UNCLAIMED for self arrived
    }
}
