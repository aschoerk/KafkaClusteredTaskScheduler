package net.oneandone.kafka.clusteredjobs;

/**
 * A signal not sent to other nodes. Used to handle spontanous internal state-changes
 */
class InternalSignal extends Signal {
    /**
     * create an internal signal
     * @param task the receiving task
     * @param signal the internal signal
     */
    public InternalSignal(TaskImpl task, SignalEnum signal) {
        super(task, signal);
        this.nodeProcThreadId = task.getNode().getUniqueNodeId();
    }
}
