package net.oneandone.kafka.clusteredjobs.support;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.KctmException;
import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefaults;

/**
 * @author aschoerk
 */
public class HeartBeatTask extends TaskDefaults {
    static Logger logger = LoggerFactory.getLogger("HeartBeatTestTask");

    static Map<String, AtomicBoolean> runningMap = Collections.synchronizedMap(new HashMap<>());
    private String name = "KafkaClusteredTaskManagerHeartBeat";
    private Duration period = Duration.ofMillis(200);

    private Duration heartBeatDuration = period.dividedBy(5);

    private HeartBeatTask() {
    }

    AtomicBoolean getRunning() {
        if(!runningMap.containsKey(getName())) {
            runningMap.put(getName(), new AtomicBoolean(false));
        }
        return runningMap.get(getName());
    }

    public Duration getHeartBeatDuration() {
        if(heartBeatDuration == null) {
            return Duration.ofMillis(100);
        }
        else {
            return heartBeatDuration;
        }
    }

    public void setHeartBeatDuration(final Duration heartBeatDuration) {
        this.heartBeatDuration = heartBeatDuration;
    }


    @Override
    public Duration getPeriod() {
        if(period == null) {
            period = Duration.ofMillis(500);
        }
        return period;
    }

    @Override
    public String getName() {
        if(name == null) {
            name = "TestTask";
        }
        return name;
    }

    @Override
    public Runnable getCode(final Node node) {
        return () -> {
            final String uniqueNodeId = node.getUniqueNodeId();
            if(!getRunning().compareAndSet(false, true)) {
                    logger.error("heartbeat on {} started a second time", uniqueNodeId);
            }
            logger.info("starting Heartbeat on {}", uniqueNodeId);
            try {
                Thread.sleep(this.getHeartBeatDuration().toMillis());
            } catch (InterruptedException e) {
                throw new KctmException("Heartbeat waiting interrupted", e);
            }
            logger.info("ending   Heartbeat on {}", uniqueNodeId);
            if(!getRunning().compareAndSet(true, false)) {
                logger.error("heartbeat on {} ended  a second time", uniqueNodeId);
            }

        };
    }


    public static final class HeartBeatTaskBuilder {
        private Instant initialTimestamp;
        private Duration maxDuration;
        private int maximumUncompletedExecutionsOnNode;
        private Duration claimedSignalPeriod;
        private Long maxExecutionsOnNode;
        private Duration resurrectionInterval;
        private String name;
        private Duration period;
        private Duration heartBeatDuration;

        private HeartBeatTaskBuilder() {}

        public static HeartBeatTaskBuilder aHeartBeatTask() {return new HeartBeatTaskBuilder();}

        public HeartBeatTaskBuilder withInitialTimestamp(Instant initialTimestamp) {
            this.initialTimestamp = initialTimestamp;
            return this;
        }

        public HeartBeatTaskBuilder withMaxDuration(Duration maxDuration) {
            this.maxDuration = maxDuration;
            return this;
        }

        public HeartBeatTaskBuilder withMaximumUncompletedExecutionsOnNode(int maximumUncompletedExecutionsOnNode) {
            this.maximumUncompletedExecutionsOnNode = maximumUncompletedExecutionsOnNode;
            return this;
        }

        public HeartBeatTaskBuilder withClaimedSignalPeriod(Duration claimedSignalPeriod) {
            this.claimedSignalPeriod = claimedSignalPeriod;
            return this;
        }

        public HeartBeatTaskBuilder withMaxExecutionsOnNode(Long maxExecutionsOnNode) {
            this.maxExecutionsOnNode = maxExecutionsOnNode;
            return this;
        }

        public HeartBeatTaskBuilder withResurrectionInterval(Duration resurrectionInterval) {
            this.resurrectionInterval = resurrectionInterval;
            return this;
        }

        public HeartBeatTaskBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public HeartBeatTaskBuilder withPeriod(Duration period) {
            this.period = period;
            return this;
        }

        public HeartBeatTaskBuilder withHeartBeatDuration(Duration heartBeatDuration) {
            this.heartBeatDuration = heartBeatDuration;
            return this;
        }

        public HeartBeatTask build() {
            HeartBeatTask heartBeatTask = new HeartBeatTask();
            heartBeatTask.setHeartBeatDuration(heartBeatDuration);
            heartBeatTask.setMaximumUncompletedExecutionsOnNode(this.maximumUncompletedExecutionsOnNode);
            heartBeatTask.setClaimedSignalPeriod(this.claimedSignalPeriod);
            heartBeatTask.setResurrectionInterval(this.resurrectionInterval);
            heartBeatTask.setInitialTimestamp(this.initialTimestamp);
            heartBeatTask.period = this.period;
            heartBeatTask.setMaxExecutionsOnNode(this.maxExecutionsOnNode);
            heartBeatTask.name = this.name;
            heartBeatTask.setMaxDuration(this.maxDuration);
            return heartBeatTask;
        }
    }
}
