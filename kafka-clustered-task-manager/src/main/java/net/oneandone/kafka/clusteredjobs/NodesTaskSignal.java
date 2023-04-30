package net.oneandone.kafka.clusteredjobs;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 *
 */
public class NodesTaskSignal implements Comparable<NodesTaskSignal> {
    /**
     * identifies the classname intended to execute the task. Must support the interface
     */
    String taskName;

    /**
     * signal intention of a statechange
     */
    TaskSignalEnum signal;

    /**
     * identifies an instance able to execute the task. The classname must be available and be runnable
     */
    String nodeProcThreadId;

    /**
     * Timestamp when this taskstate was sent to the topic
     */
    Instant timestamp;

    private transient Long currentOffset;

    private transient boolean handled = false;

    Clock clock;

    public Optional<Long> getCurrentOffset() {
        return Optional.ofNullable(currentOffset);
    }

    public void setCurrentOffset(final long currentOffsetP) {
        this.currentOffset = currentOffsetP;
    }

    boolean before(NodesTaskSignal nodesTaskSignal) {
        if (currentOffset == null || nodesTaskSignal.currentOffset == null) {
            throw new KctmException("Before only possible if offset is set in signal after receiving.");
        }
        return currentOffset < nodesTaskSignal.currentOffset;
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        NodesTaskSignal that = (NodesTaskSignal) o;
        return taskName.equals(that.taskName) && nodeProcThreadId.equals(that.nodeProcThreadId);
    }



    @Override
    public int hashCode() {
        return Objects.hash(taskName, nodeProcThreadId);
    }

    @Override
    public String toString() {
        return "NodesTaskSignal{" +
               "taskName='" + taskName + '\'' +
               ", signal=" + signal +
               ", nodeProcThreadId='" + nodeProcThreadId + '\'' +
               ", timestamp=" + timestamp +
               '}';
    }

    @Override
    public int compareTo(final NodesTaskSignal o) {
        if (this.currentOffset == null || o.currentOffset == null || this.currentOffset == o.currentOffset) {
            throw new KctmException("NodeTaskSignal not comparable if offset is not set");
        } else {
            return this.currentOffset.compareTo(o.currentOffset);
        }
    }

    public boolean isHandled() {
        return handled;
    }
}
