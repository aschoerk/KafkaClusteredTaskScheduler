package net.oneandone.kafka.clusteredjobs;

/**
 * implemented by Classes executed by Jobs which should react to undeployment or shutdown.
 */
public interface Stoppable extends Runnable {

    void setRunning();
    void shutdown();

    void run();

    boolean isRunning();
}
