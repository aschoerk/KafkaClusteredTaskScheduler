package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import net.oneandone.kafka.clusteredjobs.api.Node;

/**
 *  Signal represents Kafka-Events used to initiate state-transitions on Nodes
 */
public class Signal implements Comparable<Signal> {
    /**
     * The name of the task for which the state change is initiated
     */
    String taskName;

    /**
     * controls the statechange
     */
    SignalEnum signal;

    /**
     * the originator of the Signal
     */
    String nodeProcThreadId;

    /**
     * Timestamp when this signal was sent to the topic
     */
    Instant timestamp;

    /**
     * the offset in the one!!! partition of the topic where the signal was transported
     */
    private transient Long currentOffset;

    /**
     * used by SignalHandler to signify already processed signals
     */
    private transient boolean handled = false;

    protected Signal(final Task task, final SignalEnum signal) {
        this.taskName = task.getDefinition().getName();
        this.signal = signal;
        this.nodeProcThreadId = task.getNode().getUniqueNodeId();
        this.timestamp = task.getNode().getNow();
        this.currentOffset = -1L;
    }

    public Signal() {

    }

    public Optional<Long> getCurrentOffset() {
        return Optional.ofNullable(currentOffset);
    }

    public void setCurrentOffset(final long currentOffsetP) {
        this.currentOffset = currentOffsetP;
    }


    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        Signal that = (Signal) o;
        return taskName.equals(that.taskName) && nodeProcThreadId.equals(that.nodeProcThreadId);
    }



    @Override
    public int hashCode() {
        return Objects.hash(taskName, nodeProcThreadId);
    }

    @Override
    public String toString() {
        return "Signal{" +
               "taskName='" + taskName + '\'' +
               ", signal=" + signal +
               ", nodeProcThreadId='" + nodeProcThreadId + '\'' +
               ", timestamp=" + timestamp +
               '}';
    }

    @Override
    public int compareTo(final Signal o) {
        if (this.currentOffset == null || o.currentOffset == null || this.currentOffset == o.currentOffset) {
            throw new KctmException("NodeTaskSignal not comparable if offset is not set");
        } else {
            return this.currentOffset.compareTo(o.currentOffset);
        }
    }

    public boolean isHandled() {
        return handled;
    }

    public boolean equalNode(final Node node) {
        return node.getUniqueNodeId().equals(this.nodeProcThreadId);
    }


}
