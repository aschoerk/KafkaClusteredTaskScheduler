package net.oneandone.kafka.clusteredjobs;

/**
 * implemented by Classes executed by Jobs which should react to undeployment or shutdown.
 */
public interface Stoppable extends Runnable {

    /**
     * indicate that initialization is completed.
     */
    void setRunning();

    /**
     * initiate shutting down of the Thread/Job/Component
     */
    void shutdown();

    /**
     * setRunning has been called
     * @return setRunning has been called
     */
    boolean isRunning();
}
