package net.oneandone.kafka.clusteredjobs.api;

/**
 * Used to support container characteristics like Thread-Management.
 */
public interface Container {

    /**
     * return the name of the topic to use for synchronization. Must have exactly one partition
     * @return  the name of the topic to use for synchronization. Must have exactly one partition
     */
    String getSyncTopicName();

    /**
     * the kafka bootstrapservers
     * @return the kafka bootstrapservers
     */
    String getBootstrapServers();

    /**
     * Allows to use the Container-Threadpooling.
     * @param runnable The runnable to execute when starting the thread
     * @return the thread created in the container environment
     */
    Thread createThread(Runnable runnable);

}
