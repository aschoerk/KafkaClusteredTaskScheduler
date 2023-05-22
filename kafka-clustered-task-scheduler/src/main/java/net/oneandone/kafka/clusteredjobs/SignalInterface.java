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

}
