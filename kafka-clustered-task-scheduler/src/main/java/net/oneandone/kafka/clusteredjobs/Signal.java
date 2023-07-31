package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import net.oneandone.kafka.clusteredjobs.api.Node;

/**
 * the signal sent by nodes via 1 partition topic to control the states on the other nodes.
 */
public class Signal implements Comparable<Signal> {

    private String taskName;
    private SignalEnum signal;
    private String nodeProcThreadId;
    private Instant timestamp;
    private Long reference = null;
    /**
     * the offset in the one!!! partition of the topic where the signal was transported
     */
    private transient Long currentOffset;

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
     *
     * @param sender    the uniqueNodeId of the sender
     * @param taskName  the task the signal is related to
     * @param signal    the SignalEnum
     * @param timestamp the time the signal has been created.
     * @param reference the offset of the signal (UNCLAIMED) we are referring the CLAIMING
     */
    public Signal(final String sender, String taskName, final SignalEnum signal, Instant timestamp, Long reference) {
        this.setTaskName(taskName);
        this.setSignal(signal);
        this.setNodeProcThreadId(sender);
        this.setTimestamp(timestamp);
        this.setReference(reference);
        this.currentOffset = -1L;
    }

    /**
     * the name of the task for which the signal got sent.
     *
     * @return the name of the task for which the signal got sent.
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * the signal-enum controls the statechange of the task
     *
     * @return the enum of the signal controls the statechange of the task
     */
    public SignalEnum getSignal() {
        return signal;
    }


    /**
     * the node which sent the signal
     *
     * @return the node which sent the signal
     */
    public String getNodeProcThreadId() {
        return nodeProcThreadId;
    }

    public void setNodeProcThreadId(final String nodeProcThreadId) {
        this.nodeProcThreadId = nodeProcThreadId;
    }

    /**
     * the time when the signal was created
     *
     * @return the time when the signal was created
     */
    public Instant getTimestamp() {
        return timestamp;
    }


    /**
     * get offset as indicated from sync-topic
     *
     * @return the offset in singular partition of sync-topic
     */
    public Optional<Long> getCurrentOffset() {
        return Optional.ofNullable(currentOffset);
    }

    /**
     * set offset in partition if signal arrived from sync-topic
     *
     * @param currentOffsetP the offset in partition if signal arrived from sync-topic
     */
    public void setCurrentOffset(final long currentOffsetP) {
        this.currentOffset = currentOffsetP;
    }

    /**
     * the offset of a signal which should have been previously received which isreferred (CLAIMING)
     *
     * @return the offset of a signal which should have been previously received which isreferred (CLAIMING)
     */
    public Long getReference() {
        return reference;
    }

    /**
     * set the offset of a signal which should have been previously received which isreferred (CLAIMING)
     *
     * @param reference the offset of a signal which should have been previously received which isreferred (CLAIMING)
     */
    public void setReference(final Long reference) {
        this.reference = reference;
    }

    @Override
    public boolean equals(final Object obj) {
        if(this == obj) {
            return true;
        }
        if((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Signal that = (Signal) obj;
        return getTaskName().equals(that.getTaskName()) && getNodeProcThreadId().equals(that.getNodeProcThreadId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTaskName(), getNodeProcThreadId());
    }

    @Override
    public String toString() {
        return "Signal{" +
               "taskName='" + getTaskName() + '\'' +
               ", signal=" + getSignal() +
               ", nodeProcThreadId='" + getNodeProcThreadId() + '\'' +
               ", timestamp=" + getTimestamp() +
               ", offset=" + getCurrentOffset().orElse(-1L) +
               '}';
    }

    @Override
    public int compareTo(final Signal o) {
        if((this.currentOffset == null) || (o.currentOffset == null) || Objects.equals(this.currentOffset, o.currentOffset)) {
            throw new KctmException("NodeTaskSignal not comparable if offset is not set");
        }
        else {
            return this.currentOffset.compareTo(o.currentOffset);
        }
    }

    /**
     * check if signal is sent by a certain node
     *
     * @param node the node to be checked the signal for
     * @return true of th signal was sent by node.
     */
    public boolean equalNode(final Node node) {
        return node.getUniqueNodeId().equals(this.getNodeProcThreadId());
    }

    /**
     * set the name of the task for which the signal is created /sent
     * @param taskName the name of the task for which the signal is created /sent
     */
    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * set the signal-id  used to control the state-changes of a task-instance
     * @param signal the id used to control the state-changes of a task-instance
     */
    public void setSignal(SignalEnum signal) {
        this.signal = signal;
    }

    /**
     * set the timestamp of the signal
     * @param timestamp the timestamp the signal was created
     */
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
}
