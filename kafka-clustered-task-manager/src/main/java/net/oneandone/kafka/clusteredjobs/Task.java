package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;

/**
 * @author aschoerk
 */
public class Task implements net.oneandone.kafka.clusteredjobs.api.Task {

    private final TaskDefinition taskDefinition;
    Logger logger = LoggerFactory.getLogger(TaskBase.class);

    private TaskStateEnum currentState;

    private Instant lastClaimedInfo;

    private Instant lastStartup;

    private net.oneandone.kafka.clusteredjobs.api.Node node;

    int executionsOnNode = 0;

    Task(TaskDefinition taskDefinition) {
        this.taskDefinition = taskDefinition;
    }

    public TaskStateEnum getLocalState() {
        return currentState;
    }

    public void setLocalState(final TaskStateEnum stateToSet) {
        logger.info("Task {} Setting state: {} from state: {}", taskDefinition.getName(), stateToSet, getLocalState());
        switch (stateToSet) {
            case INITIATING:
            case ERROR:
            case HANDLING_BY_OTHER:
            case CLAIMED_BY_OTHER:
                executionsOnNode = 0;
                sawClaimedInfo();
                break;
            case HANDLING_BY_NODE:
                if(currentState != TaskStateEnum.HANDLING_BY_NODE) {
                    executionsOnNode++;
                    lastStartup = getNow();
                }
                break;
        }
        currentState = stateToSet;
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
        if(currentState.equals(TaskStateEnum.HANDLING_BY_NODE)) {
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

    public void setNode(final net.oneandone.kafka.clusteredjobs.api.Node node) {
        if(this.node != null) {
            if(node.getUniqueNodeId().equals(this.node.getUniqueNodeId())) {
                throw new KctmException("Setting NodeImpl in Task possible only once.");
            }
        }
        this.node = node;
    }

    @Override
    public String toString() {
        return "Task{" +
               "currentState=" + currentState +
               '}';
    }

    public TaskDefinition getDefinition() {
        return taskDefinition;
    }
}
