package net.oneandone.kafka.clusteredjobs;

import net.oneandone.kafka.clusteredjobs.api.ClusterTask;

/**
 * @author aschoerk
 */
public class ClusterTaskImpl implements ClusterTask {
    private final Runnable startup;
    private final Runnable theTask;
    private final Runnable shutdown;

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
