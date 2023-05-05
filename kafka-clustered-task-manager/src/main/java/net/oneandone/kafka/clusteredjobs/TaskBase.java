package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
abstract class TaskBase implements Task {
    Logger logger = LoggerFactory.getLogger(TaskBase.class);

    private TaskStateEnum currentState;
    private Instant lastClaimedInfo;
    private Instant lastStartup;

    int executionsOnNode = 0;

    @Override
    public TaskStateEnum getLocalState() {
        return currentState;
    }

    @Override
    public void setLocalState(final TaskStateEnum stateToSet) {
        logger.info("Task {} Setting state: {} from state: {}", this.getName(),  stateToSet, getLocalState());
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
                    executionsOnNode ++;
                    lastStartup = getNow();
                }
                break;
        }
        currentState = stateToSet;
    }

    private Instant getNow() {
        return getNode().getNow();
    }

    @Override
    public Instant getLastClaimedInfo() {
        return lastClaimedInfo;
    }

    @Override
    public void sawClaimedInfo() {
        this.lastClaimedInfo = getNow();
    }

    @Override
    public Instant getHandlingStarted() {
        if(currentState.equals(TaskStateEnum.HANDLING_BY_NODE)) {
            return lastStartup;
        }
        else {
            throw new KctmException("Should not ask for handlingStarted if not HANDLING");
        }
    }

    @Override
    public Instant getLastStartup() {
        return lastStartup;
    }

    @Override
    public long getExecutionsOnNode() {
        return executionsOnNode;
    }

    @Override
    public String toString() {
        return "TaskBase{" +
               "currentState=" + currentState +
               '}';
    }
}
