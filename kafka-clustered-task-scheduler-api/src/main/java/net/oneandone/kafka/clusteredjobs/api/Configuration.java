package net.oneandone.kafka.clusteredjobs.api;

import java.time.Duration;

/**
 * @author aschoerk
 */
public interface Configuration {

    /**
     * signifies the period in which a node which did not send messages otherwise is to sent a heartbeat signal via kafka
     * @return the period in which a node which did not send messages otherwise is to sent a heartbeat signal via kafka
     */
    default Duration getNodeHeartBeat() { return Duration.ofSeconds(30); }


    /**
     * The period's foundation is established by how frequently the Reviver, running in a node, checks for suspiciously
     * long-claimed tasks by other nodes without receiving any signals from them.
     * @return The period's foundation for checking if tasks are yet alive.
     */
    default Duration getReviverPeriod() { return Duration.ofSeconds(60); }
}
