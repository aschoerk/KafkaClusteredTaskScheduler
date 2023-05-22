package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.states.StateEnum.CLAIMED_BY_OTHER;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.INITIATING;
import static net.oneandone.kafka.clusteredjobs.states.StateEnum.UNCLAIMING;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.KctmException;
import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.Task;

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

    protected static void claiming(final Task task, final Signal s) {
        if (s.getReference() == null && s.getReference() == task.getUnclaimedSignalOffset() ||
            s.getReference() != null && s.getReference().equals(task.getUnclaimedSignalOffset())) {
            task.setLocalState(CLAIMED_BY_OTHER, s);
        }
    }

    void info(Task task, Signal s, String message) {
        logger.info("N: {} T: {}/{}  S: {}/{}/{} {}", node.getUniqueNodeId(), task.getDefinition().getName(), task.getLocalState(),
                s.getNodeProcThreadId(), s.getSignal(), s.getCurrentOffset().orElse(-1L),message);
    }
    void error(Task task, Signal s, String message) {
        logger.error("N: {} T: {}/{}  S: {}/{}/{} {}", node.getUniqueNodeId(), task.getDefinition().getName(), task.getLocalState(),
                s.getNodeProcThreadId(), s.getSignal(), s.getCurrentOffset().orElse(-1L),message);
        task.setLocalState(ERROR);
    }

    public void handle(Task task, Signal s) {
        if (task.getLocalState() != state) {
            throw new KctmException("Expected states to match between statehandler and task");
        }
        if (s.getSignal().isInternal()) {
            info(task, s, "internal");
            handleInternal(task, s);
        }
        else if (node.getUniqueNodeId().equals(s.getNodeProcThreadId())) {
            info(task, s, "to Me");
            handleOwnSignal(task, s);
        } else {
            info(task, s, "event");
            handleSignal(task, s);
        }
    }

    protected void unclaimed(final Task task, Signal s) {
        task.setUnclaimedSignalOffset(s.getCurrentOffset().get());
        task.setLocalState(INITIATING);
        node.getPendingHandler().scheduleTaskForClaiming(task);
        node.getPendingHandler().removeTaskResurrection(task);
    }

    protected void handleOwnSignal(final Task task, final Signal s) {
        error(task, s, "did not expect own signal in this state");
    }

    protected void handleInternal(Task task, Signal s) {
        error(task, s, "did not expect internal signal in this state");
    }

    protected void handleSignal(final Task task, final Signal s) {
        error(task, s, "did not expect foreign signal in this state");
    }

    public NodeImpl getNode() {
        return node;
    }

    public StateEnum getState() {
        return state;
    }

    protected void claimed(final Task task, final Signal s) {
        final Optional<String> currentExecutor = task.getCurrentExecutor();
        if(currentExecutor.isPresent() && !s.getNodeProcThreadId().equals(currentExecutor.get())) {
            info(task, s, "Executor " + currentExecutor.get() + " different");
        } else {
            task.setLocalState(CLAIMED_BY_OTHER, s.getNodeProcThreadId());
            task.sawClaimedInfo();
        }

    }

    protected void startUnclaiming(final Task task) {
        getNode().getPendingHandler().removeTaskStarter(task);
        getNode().getPendingHandler().removeClaimedHeartbeat(task);
        task.setLocalState(UNCLAIMING);
        getNode().getSender().sendSignal(task, SignalEnum.UNCLAIMED);
        // this can set task to CLAIMING before UNCLAIMED arrives but for following states,
        // triggering events must happen after the UNCLAIMED for self arrived
    }
}
