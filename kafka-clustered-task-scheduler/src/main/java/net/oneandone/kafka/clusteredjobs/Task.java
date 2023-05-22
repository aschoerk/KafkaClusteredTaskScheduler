package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.states.StateEnum.HANDLING_BY_OTHER;

import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;
import net.oneandone.kafka.clusteredjobs.states.StateEnum;

/**
 * @author aschoerk
 */
public class Task implements net.oneandone.kafka.clusteredjobs.api.Task {

    private final TaskDefinition taskDefinition;
    Logger logger = LoggerFactory.getLogger(Task.class);

    StateEnum localState;

    private Long unclaimedSignalOffsetl = null;


    Instant lastClaimedInfo;

    Instant lastStartup;

    String currentExecutor;

    private net.oneandone.kafka.clusteredjobs.api.Node node;

    int executionsOnNode = 0;

    Task(Node node, TaskDefinition taskDefinition) {
        this.node = node;
        this.taskDefinition = taskDefinition;
    }

    public StateEnum getLocalState() {
        return localState;
    }

    public void setLocalState(final StateEnum stateToSet, Signal s) {
        setLocalState(stateToSet, s.nodeProcThreadId);
    }
    public void setLocalState(final StateEnum stateToSet, final String nodeName) {
        logger.info("N: {} T: {} Setting State: {} from state: {} because of node: {}", node.getUniqueNodeId(), taskDefinition.getName(),  stateToSet, localState, this.getCurrentExecutor().orElse("null"));
        if (stateToSet == HANDLING_BY_OTHER || stateToSet == StateEnum.CLAIMED_BY_OTHER) {
            currentExecutor = nodeName;
            executionsOnNode = 0;
            sawClaimedInfo();
            localState = stateToSet;
        } else {
            throw new KctmException("Only deliver NodeName if CLAIMED or HANDLED by Other");
        }
    }


    public void setLocalState(final StateEnum stateToSet) {
        logger.info("Node: {} Task {} Setting state: {} from state: {}",node.getUniqueNodeId(), taskDefinition.getName(), stateToSet, getLocalState());
        switch (stateToSet) {
            case HANDLING_BY_OTHER:
            case CLAIMED_BY_OTHER:
                throw new KctmException("If setting state to claimed or handling by other, add signal.");
            case INITIATING:
            case ERROR:
                executionsOnNode = 0;
                sawClaimedInfo();
                break;
            case HANDLING_BY_NODE:
                if(localState != StateEnum.HANDLING_BY_NODE) {
                    executionsOnNode++;
                    lastStartup = getNow();
                }
                break;
            case CLAIMED_BY_NODE:
                currentExecutor = null;
                break;
        }
        localState = stateToSet;
    }

    private Instant getNow() {
        return getNode().getNow();
    }

    public Instant getLastClaimedInfo() {
        return lastClaimedInfo;
    }

    public void sawClaimedInfo() {
        this.lastClaimedInfo = getNow();
    }

    public Instant getHandlingStarted() {
        if(localState.equals(StateEnum.HANDLING_BY_NODE)) {
            return lastStartup;
        }
        else {
            throw new KctmException("Should not ask for handlingStarted if not HANDLING");
        }
    }

    public Instant getLastStartup() {
        return lastStartup;
    }

    public long getExecutionsOnNode() {
        return executionsOnNode;
    }

    public net.oneandone.kafka.clusteredjobs.api.Node getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "Task{" +
               "currentState=" + localState +
               '}';
    }

    public TaskDefinition getDefinition() {
        return taskDefinition;
    }

    public Optional<String> getCurrentExecutor() {
        return Optional.ofNullable(currentExecutor);
    }

    public void clearExecutionsOnNode() {
        executionsOnNode = 0;
    }

    public Long getUnclaimedSignalOffset() {
        return unclaimedSignalOffsetl;
    }

    public void setUnclaimedSignalOffset(final Long unclaimedSignalOffsetl) {
        this.unclaimedSignalOffsetl = unclaimedSignalOffsetl;
    }

}
