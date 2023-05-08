package net.oneandone.kafka.clusteredjobs;

/**
 * @author aschoerk
 */
public class InternalSignal extends Signal {
    public InternalSignal(Task task, SignalEnum signal) {
        super(task, signal);
    }
}
