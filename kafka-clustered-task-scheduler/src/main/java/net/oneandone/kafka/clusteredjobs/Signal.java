package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import net.oneandone.kafka.clusteredjobs.api.Node;


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
     * The offset of a signal, this one refers to.
     */
    Long reference = null;

    /**
     * the offset in the one!!! partition of the topic where the signal was transported
     */
    private transient Long currentOffset;

    /**
     * used by SignalHandler to signify already processed signals
     */
    private transient boolean handled = false;

    Signal(final TaskImpl task, final SignalEnum signal) {
        this(task.getNode().getUniqueNodeId(), task.getDefinition().getName(), signal, task.getNode().getNow(), null);
        this.currentOffset = -1L;
    }

    /**
     * necessary for creation after Signal arrived from sync-topic
     */
    public Signal() {

    }

    /**
     * constructor used internally if signal for an unknown task arrived
     * @param sender the uniqueNodeId of the sender
     * @param taskName the task the signal is related to
     * @param signal the SignalEnum
     * @param timestamp the time the signal has been created.
     * @param reference the offset of the signal (UNCLAIMED) we are referring the CLAIMING
     */
    public Signal(final String sender, String taskName, final SignalEnum signal, Instant timestamp, Long reference) {
        this.taskName = taskName;
        this.signal = signal;
        this.nodeProcThreadId = sender;
        this.timestamp = timestamp;
        this.reference = reference;
        this.currentOffset = -1L;
    }

    /**
     * the name of the task for which the signal got sent.
     * @return the name of the task for which the signal got sent.
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * the signal-enum
     * @return the enum of the signal
     */
    public SignalEnum getSignal() {
        return signal;
    }


    /**
     * the node which sent the signal
     * @return the node which sent the signal
     */
    public String getNodeProcThreadId() {
        return nodeProcThreadId;
    }

    /**
     * the time when the signal was created
     * @return the time when the signal was created
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * the signal was handled
     * @return the signal was handled
     */
    public boolean isHandled() {
        return handled;
    }

    /**
     * get offset as indicated from sync-topic
     * @return the offset in singular partition of sync-topic
     */
    public Optional<Long> getCurrentOffset() {
        return Optional.ofNullable(currentOffset);
    }

    /**
     * set offset in partition if signal arrived from sync-topic
     * @param currentOffsetP the offset in partition if signal arrived from sync-topic
     */
    public void setCurrentOffset(final long currentOffsetP) {
        this.currentOffset = currentOffsetP;
    }

    /**
     * the offset of a signal which should have been previously received which isreferred (CLAIMING)
     * @return the offset of a signal which should have been previously received which isreferred (CLAIMING)
     */
    public Long getReference() {
        return reference;
    }

    /**
     * set the offset of a signal which should have been previously received which isreferred (CLAIMING)
     * @param reference the offset of a signal which should have been previously received which isreferred (CLAIMING)
     */
    public void setReference(final Long reference) {
        this.reference = reference;
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
               ", offset=" + getCurrentOffset().orElse(-1L) +
               '}';
    }

    @Override
    public int compareTo(final Signal o) {
        if (this.currentOffset == null || o.currentOffset == null || Objects.equals(this.currentOffset, o.currentOffset)) {
            throw new KctmException("NodeTaskSignal not comparable if offset is not set");
        } else {
            return this.currentOffset.compareTo(o.currentOffset);
        }
    }


    /**
     * check if signal is sent by a certain node
     * @param node the node to be check the signal for
     * @return true of th signal was sent by node.
     */
    public boolean equalNode(final Node node) {
        return node.getUniqueNodeId().equals(this.nodeProcThreadId);
    }


}
