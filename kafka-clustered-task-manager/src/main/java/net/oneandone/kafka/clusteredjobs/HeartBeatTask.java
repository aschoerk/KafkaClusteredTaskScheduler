package net.oneandone.kafka.clusteredjobs;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
class HeartBeatTask extends TaskBase implements Task {

    private static AtomicBoolean running = new AtomicBoolean(false);

    private final Node node;
    Logger log = LoggerFactory.getLogger(Node.class);

    HeartBeatTask(Node node) {
        this.node = node;
    }

    @Override
    public Instant getInitialTimestamp() {
        return Instant.ofEpochMilli(1682074899000L);
    }

    @Override
    public Duration getInitialDelay() {
        return Duration.ofMillis(10000);
    }


    @Override
    public Duration getPeriod() {
        return Duration.ofMillis(200);
    }

    @Override
    public Duration getClaimedSignalPeriod() {
        return Duration.ofSeconds(1);
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
    public Node getNode() {
        return node;
    }

    static AtomicBoolean entered = new AtomicBoolean(false);

    @Override
    public Runnable getJob() {
        return () -> {
            if (!entered.compareAndSet(false, true)) {
                logger.error("expected false entering heartbeat");
            }
            final String uniqueNodeId = node.getUniqueNodeId();
            if (!running.compareAndSet(false, true)) {
                log.error("heartbeat on {} started a second time", uniqueNodeId);
            }
            log.info("starting Heartbeat on {}", uniqueNodeId);
            try {
                Thread.sleep(this.getPeriod().toMillis() / 5);
            } catch (InterruptedException e) {
                throw new KctmException("Heartbeat waiting interrupted", e);
            }
            log.info("ending   Heartbeat on {}", uniqueNodeId);
            if (!running.compareAndSet(true, false)) {
                log.error("heartbeat on {} ended  a second time", uniqueNodeId);
            }
            if (!entered.compareAndSet(true, false)) {
                logger.error("expected true exiting heartbeat");
            }

        };
    }




}
