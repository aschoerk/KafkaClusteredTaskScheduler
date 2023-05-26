package net.oneandone.kafka.clusteredjobs.api;

/**
 * @author aschoerk
 */
public interface Task {

    TaskDefinition getDefinition();

    StateEnum getLocalState();

}
