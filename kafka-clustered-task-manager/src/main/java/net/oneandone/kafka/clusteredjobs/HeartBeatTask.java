package net.oneandone.kafka.clusteredjobs;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
class HeartBeatTask extends TaskBase implements Task {

    private static AtomicBoolean running = new AtomicBoolean(false);

    private final String node;
    Logger log = LoggerFactory.getLogger(Node.class);

    HeartBeatTask(String node) {
        this.node = node;
    }

    @Override
    public long getInitialTimestamp() {
        return 1682074899000L;
    }

    @Override
    public long getInitialDelay() {
        return 10000;
    }


    @Override
    public long getPeriod() {
        return 5000;
    }

    @Override
    public long maxExecutionsOnNode() {
        return 5;
    }

    @Override
    public String getName() {
        return "KafkaClusteredTaskManagerHeartBeat";
    }

    @Override
    public Runnable getJob() {
        return () -> {
            if (!running.compareAndSet(false, true)) {
                log.error("heartbeat on {} started a second time", node);
            }
            log.info("starting Heartbeat on {}",node);
            try {
                Thread.sleep(this.getPeriod() / 5);
            } catch (InterruptedException e) {
                throw new KctmException("Heartbeat waiting interrupted", e);
            }
            log.info("ending   Heartbeat on {}", node);
            if (!running.compareAndSet(true, false)) {
                log.error("heartbeat on {} ended  a second time", node);
            }

        };
    }
}
