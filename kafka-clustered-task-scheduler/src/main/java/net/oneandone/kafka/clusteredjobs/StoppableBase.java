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
    private static Logger logger = LoggerFactory.getLogger(StoppableBase.class);
    private AtomicBoolean doShutdown = new AtomicBoolean(false);
    private boolean running = false;


    @Override
    public void shutdown() {
        this.doShutdown.set(true);
        if(scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    /**
     * ask if a Stoppable should shut down
     * @return true if the stoppable shut shutdown.
     */
    protected boolean doShutdown() {
        if(doShutdown.get()) {
            logger.trace("doShutdown true");
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * initialize a threadname so that it is uniquely recognizable
     * @param name the base of the generated threadname.
     */
    public static void initThreadName(final String name) {
        String threadName = String.format("KCTM_%010d_%05d_%s_%5d",
                Thread.currentThread().getContextClassLoader().hashCode(),
                Thread.currentThread().getId(),
                String.format("%-7s",name).substring(0,7), threadIx.incrementAndGet());
        Thread.currentThread().setName(threadName);
        logger.trace("Initialized Name of Thread with Id: {}", Thread.currentThread().getId());
    }

    @Override
    public void setRunning() {
        this.running = true;
    }

    @Override
    public boolean isRunning() {
        return running && !doShutdown.get();
    }

}
