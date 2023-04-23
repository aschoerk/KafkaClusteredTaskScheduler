package net.oneandone.kafka.clusteredjobs;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Baseclass of classes which should react to undeployment or shutdown as fast as possible
 */
public abstract class StoppableBase implements Stoppable {
    private static AtomicInteger threadIx = new AtomicInteger();
    private ScheduledFuture<?> scheduledFuture = null;
    private Logger logger = LoggerFactory.getLogger(StoppableBase.class);
    private AtomicBoolean doShutdown = new AtomicBoolean(false);

    public ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }

    public void setScheduledFuture(final ScheduledFuture<?> scheduledFutureP) {
        if(this.scheduledFuture != null) {
            logger.error("Expected scheduledFuture to be null, should init only once");
        }
        this.scheduledFuture = scheduledFutureP;
    }

    @Override
    public void shutdown() {
        this.doShutdown.set(true);
        if(scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    protected boolean doShutdown() {
        if(doShutdown.get()) {
            logger.trace("doShutdown true");
            return true;
        }
        else {
            return false;
        }
    }

    protected void initThreadName(final String name) {
        String threadName = String.format("KCTM_%010d_%05d_%s_%5d",
                Thread.currentThread().getContextClassLoader().hashCode(),
                Thread.currentThread().getId(),
                String.format("%-7s",name).substring(0,7), threadIx.incrementAndGet());
        Thread.currentThread().setName(threadName);
        logger.trace("Initialized Name of Thread with Id: {}", Thread.currentThread().getId());
    }

    int getSleepTime() {
        return 100;
    }

}
