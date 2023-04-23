package net.oneandone.kafka.clusteredjobs;

/**
 * @author aschoerk
 */
public enum TaskSignalEnum {
    /**
     *  signal Task is initiated, who is able to execute it may try to claim it
     *  if multiple node do initiating, check if parameters
     */
    INITIATING,
    CLAIMING,   //
    CLAIMED_BY_ME,
    HANDLED_BY_ME,
    UNCLAIMED
}
