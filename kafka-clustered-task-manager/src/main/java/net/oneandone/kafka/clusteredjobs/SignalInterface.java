package net.oneandone.kafka.clusteredjobs;

/**
 * @author aschoerk
 */
public interface SignalInterface {
    default boolean isInternal() { return false; }

    void handle(SignalHandler signalHandler, Task task, Signal s);
}
