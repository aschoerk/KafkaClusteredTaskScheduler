package net.oneandone.kafka.clusteredjobs.api;

import java.time.Duration;
import java.util.concurrent.Future;

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
    Future submitInThread(Runnable runnable);

    /**
     * Allows to use the Container-Threadpooling.
     * @param runnable The runnable to execute when starting the thread
     * @return the thread created in the container environment
     */
    Future submitInLongRunningThread(Runnable runnable);


    default Configuration getConfiguration() { return new Configuration() {
        @Override
        public Duration getNodeHeartBeat() {
            return Duration.ofSeconds(1);
        }
    }; }

}
