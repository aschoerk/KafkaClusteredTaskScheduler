package net.oneandone.kafka.clusteredjobs.support;

import java.time.Duration;
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

    static Map<String, AtomicBoolean> runningMap = Collections.synchronizedMap(new HashMap<>());

    private String name = "KafkaClusteredTaskManagerHeartBeat";
    static Logger log = LoggerFactory.getLogger(Node.class);
    private Duration period = Duration.ofMillis(200);

    private Duration heartBeatDuration = period.dividedBy(5);

    public HeartBeatTask() {
    }

    AtomicBoolean getRunning() {
        if (!runningMap.containsKey(getName())) {
            runningMap.put(getName(), new AtomicBoolean(false));
        }
        return runningMap.get(getName());
    }

    public Duration getHeartBeatDuration() {
        return heartBeatDuration;
    }

    public void setHeartBeatDuration(final Duration heartBeatDuration) {
        this.heartBeatDuration = heartBeatDuration;
    }


    @Override
    public Duration getPeriod() {
        return period;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Runnable getJob(final Node node) {
        return () -> {
            final String uniqueNodeId = node.getUniqueNodeId();
            if (!getRunning().compareAndSet(false, true)) {
                log.error("heartbeat on {} started a second time", uniqueNodeId);
            }
            log.info("starting Heartbeat on {}", uniqueNodeId);
            try {
                Thread.sleep(this.getHeartBeatDuration().toMillis());
            } catch (InterruptedException e) {
                throw new KctmException("Heartbeat waiting interrupted", e);
            }
            log.info("ending   Heartbeat on {}", uniqueNodeId);
            if (!getRunning().compareAndSet(true, false)) {
                log.error("heartbeat on {} ended  a second time", uniqueNodeId);
            }

        };
    }

    public static final class HeartBeatTaskBuilder extends TaskDefaultsBuilder {
        private String name;
        private Duration period;
        private Duration heartBeatDuration;

        private HeartBeatTaskBuilder() {}

        public static HeartBeatTaskBuilder aHeartBeatTask() {return new HeartBeatTaskBuilder();}

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
            heartBeatTask.name = this.name;
            heartBeatTask.period = this.period;
            return heartBeatTask;
        }
    }
}
