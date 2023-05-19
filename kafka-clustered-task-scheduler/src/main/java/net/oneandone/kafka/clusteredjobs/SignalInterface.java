package net.oneandone.kafka.clusteredjobs;

/**
 * The interface the SignalEnum is implementing
 */
public interface SignalInterface {
    /**
     * if true the signal will never be sent via kafka sync-topic
     * @return true: the signal will never be sent via kafka sync-topic
     */
    default boolean isInternal() { return false; }

    /**
     * call the signalHandler method executing the change initiated by this Signal
     * @param signalHandler the signalHandler to be called
     * @param task the task to be used as parameter
     * @param s the Signal as it arrived or was generated.
     */
    void handle(SignalHandler signalHandler, Task task, Signal s);
}
