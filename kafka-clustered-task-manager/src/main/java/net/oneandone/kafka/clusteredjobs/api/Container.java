package net.oneandone.kafka.clusteredjobs.api;

/**
 * Used to support container characteristics like Thread-Management.
 */
public interface Container {

    String getSyncTopicName();

    String getBootstrapServers();
    Thread createThread(Runnable runnable);

}
