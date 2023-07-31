package net.oneandone.kafka.clusteredjobs;

import net.oneandone.kafka.clusteredjobs.api.ClusterTask;

/**
 * describes an implementation of the code to be executed.
 */
public class ClusterTaskImpl implements ClusterTask {
    private final Runnable startup;
    private final Runnable theTask;
    private final Runnable shutdown;

    /**
     * initializes a Task to be initiated/ executed/unclaimed
     * @param startup to be called once after claiming
     * @param theTask the task to be called during HANDLING
     * @param shutdown to be called when unclaiming
     */
    public ClusterTaskImpl(final Runnable startup, final Runnable theTask, final Runnable shutdown) {
        this.startup = startup;
        this.theTask = theTask;
        this.shutdown = shutdown;
    }

    @Override
    public void startup() {
        if (startup != null) {
            startup.run();
        }
    }

    @Override
    public void call() {
        if (theTask != null) {
            theTask.run();
        }
    }

    @Override
    public void shutdown() {
        if (shutdown != null) {
            shutdown.run();
        }
    }
}
