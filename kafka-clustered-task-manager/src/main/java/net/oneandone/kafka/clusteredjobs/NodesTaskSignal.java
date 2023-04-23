package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.Objects;

/**
 *
 */
public class NodesTaskSignal {
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

    transient long currentOffset;


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
}
