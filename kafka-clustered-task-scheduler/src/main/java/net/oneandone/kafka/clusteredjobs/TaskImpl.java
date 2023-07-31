package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.api.StateEnum.HANDLING_BY_OTHER;

import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;
import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;

/**
 * @author aschoerk
 */
public class TaskImpl implements net.oneandone.kafka.clusteredjobs.api.Task {

    private final TaskDefinition taskDefinition;
    static final Logger logger = LoggerFactory.getLogger(TaskImpl.class);

    StateEnum localState;

    private Long unclaimedSignalOffsetl = null;


    Instant lastClaimedInfo;

    Instant lastStartup;

    String currentExecutor;

    private final net.oneandone.kafka.clusteredjobs.api.Node node;

    int executionsOnNode = 0;


    TaskImpl(Node node, TaskDefinition taskDefinition) {
        this.node = node;
        this.taskDefinition = taskDefinition;
    }

    /**
     * return the current state of the task on the node
     * @return the current state of the task on the node
     */
    @Override
    public StateEnum getLocalState() {
        return localState;
    }

    /**
     * change the state of the task, only usable for states CLAIMED_BY_OTHER, HANDLING_BY_OTHER
     * @param stateToSet state to be set now
     * @param s the signal initiating the statechange
     */
    public void setLocalState(final StateEnum stateToSet, Signal s) {
        setLocalState(stateToSet, s.getNodeProcThreadId());
    }

    /**
     * change the state of the task, only usable for states CLAIMED_BY_OTHER, HANDLING_BY_OTHER
     * @param stateToSet state to be set now
     * @param nodeName the name of the node which sent the signal initiating the statechange
     */
    public void setLocalState(final StateEnum stateToSet, final String nodeName) {
        if (logger.isInfoEnabled()) {
            logger.info("N: {} T: {} Setting State: {} from state: {} because of node: {}", node.getUniqueNodeId(),
                    taskDefinition.getName(), stateToSet, localState, this.getCurrentExecutor().orElse("null"));
        }
        if ((stateToSet == HANDLING_BY_OTHER) || (stateToSet == StateEnum.CLAIMED_BY_OTHER)) {
            currentExecutor = nodeName;
            executionsOnNode = 0;
            sawClaimedInfo();
            localState = stateToSet;
        } else {
            throw new KctmException("Only deliver NodeName if CLAIMED or HANDLED by Other");
        }
    }


    /**
     * change the state of the task, not usable for states CLAIMED_BY_OTHER, HANDLING_BY_OTHER
     * @param stateToSet state to be set now
     */
    public void setLocalState(final StateEnum stateToSet) {
        logger.info("Node: {} TaskImpl {} Setting state: {} from state: {}",node.getUniqueNodeId(), taskDefinition.getName(), stateToSet, getLocalState());
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

    /**
     * return the timestamp this task was the last time announced as claimed
     * @return the timestamp this task was the last time announced as claimed
     */
    public Instant getLastClaimedInfo() {
        return lastClaimedInfo;
    }

    /**
     * signal that the task was just been seen as claimed
     */
    public void sawClaimedInfo() {
        this.lastClaimedInfo = getNow();
    }

    /**
     * return the number of executions on the node since the last clearing of this counter
     * @return the number of executions on the node since the last clearing of this counter
     */
    public long getExecutionsOnNode() {
        return executionsOnNode;
    }

    /**
     * return the node where the task is potentially running
     * @return the node where the task is potentially running
     */
    public net.oneandone.kafka.clusteredjobs.api.Node getNode() {
        return node;
    }

    @Override
    public String toString() {
        return "TaskImpl{" +
               "currentState=" + localState +
               '}';
    }

    /**
     * return the fixed properties of the task
     * @return the fixed properties of the task
     */
    @Override
    public TaskDefinition getDefinition() {
        return taskDefinition;
    }

    /**
     * return the supposed current executor according to the last CLAIMED_BY_OTHER, HANDLING_BY_OTHER, CLAIMED, HEARTBEAT events.
     * @return the supposed current executor according to the last CLAIMED_BY_OTHER, HANDLING_BY_OTHER, CLAIMED, HEARTBEAT events.
     */
    public Optional<String> getCurrentExecutor() {
        return Optional.ofNullable(currentExecutor);
    }

    /**
     * reset the counter, counting the executions of the task.
     */
    public void clearExecutionsOnNode() {
        executionsOnNode = 0;
    }

    /**
     * return the offset of the unclaimed message (only one partition in sync-topicP
     * @return the offset of the unclaimed message (only one partition in sync-topicP
     */
    public Long getUnclaimedSignalOffset() {
        return unclaimedSignalOffsetl;
    }

    /**
     * to be able to match a CLAIMING to the UNCLAIMED message set the offset of the UNCLAIMED-Message
     * @param unclaimedSignalOffsetl the offset of the unclaimed message (only one partition in sync-topicP
     */
    public void setUnclaimedSignalOffset(final Long unclaimedSignalOffsetl) {
        this.unclaimedSignalOffsetl = unclaimedSignalOffsetl;
    }

}
