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
    public InternalSignal(Task task, SignalEnum signal) {
        super(task, signal);
    }
}
