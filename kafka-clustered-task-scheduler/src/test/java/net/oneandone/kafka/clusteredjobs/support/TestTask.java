package net.oneandone.kafka.clusteredjobs.support;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.KctmException;
import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefaults;

/**
 * @author aschoerk
 */
public class TestTask extends TaskDefaults {
    static Logger logger = LoggerFactory.getLogger("HeartBeatTestTask");

    static Map<String, AtomicLong> interruptedMap = Collections.synchronizedMap(new HashMap<>());

    static Map<String, AtomicBoolean> runningMap = Collections.synchronizedMap(new HashMap<>());
    private String name = "KafkaClusteredTaskManagerHeartBeat";
    private Duration period = Duration.ofMillis(200);

    private Duration handlingDuration = period.dividedBy(5);

    private AtomicLong executions = new AtomicLong(0L);

    private TestTask() {
    }

    public AtomicLong getInterrupted() {
        if (!interruptedMap.containsKey(getName())) {
            interruptedMap.put(getName(), new AtomicLong(0L));
        }
        return interruptedMap.get(getName());
    }

    AtomicBoolean getRunning() {
        if(!runningMap.containsKey(getName())) {
            runningMap.put(getName(), new AtomicBoolean(false));
        }
        return runningMap.get(getName());
    }

    public Duration getHandlingDuration() {
        if(handlingDuration == null) {
            return Duration.ofMillis(100);
        }
        else {
            return handlingDuration;
        }
    }

    public void setHandlingDuration(final Duration handlingDuration) {
        this.handlingDuration = handlingDuration;
    }

    public long getExecutions() {
        return executions.get();
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
            executions.incrementAndGet();
            final String uniqueNodeId = node.getUniqueNodeId();
            if(!getRunning().compareAndSet(false, true)) {
                    logger.error("Testtask on {} started a second time", uniqueNodeId);
            }
            logger.info("starting Testtask on {}", uniqueNodeId);
            try {
                Thread.sleep(this.getHandlingDuration().toMillis());
            } catch (InterruptedException e) {
                getInterrupted().incrementAndGet();
            }
            logger.info("ending   Testtask on {}", uniqueNodeId);
            if(!getRunning().compareAndSet(true, false)) {
                logger.error("Testtask on {} ended  a second time", uniqueNodeId);
            }

        };
    }




    public static final class TestTaskBuilder {
        private Instant initialTimestamp;
        private Duration maxDuration;
        private int maximumUncompletedExecutionsOnNode;
        private Duration claimedSignalPeriod;
        private Long maxExecutionsOnNode;
        private Duration resurrectionInterval;
        private String name;
        private Duration period;
        private Duration handlingDuration;

        private TestTaskBuilder() {}

        public static TestTaskBuilder aTestTask() {return new TestTaskBuilder();}

        public TestTaskBuilder withInitialTimestamp(Instant initialTimestamp) {
            this.initialTimestamp = initialTimestamp;
            return this;
        }

        public TestTaskBuilder withMaxDuration(Duration maxDuration) {
            this.maxDuration = maxDuration;
            return this;
        }

        public TestTaskBuilder withMaximumUncompletedExecutionsOnNode(int maximumUncompletedExecutionsOnNode) {
            this.maximumUncompletedExecutionsOnNode = maximumUncompletedExecutionsOnNode;
            return this;
        }

        public TestTaskBuilder withClaimedSignalPeriod(Duration claimedSignalPeriod) {
            this.claimedSignalPeriod = claimedSignalPeriod;
            return this;
        }

        public TestTaskBuilder withMaxExecutionsOnNode(Long maxExecutionsOnNode) {
            this.maxExecutionsOnNode = maxExecutionsOnNode;
            return this;
        }

        public TestTaskBuilder withResurrectionInterval(Duration resurrectionInterval) {
            this.resurrectionInterval = resurrectionInterval;
            return this;
        }

        public TestTaskBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public TestTaskBuilder withPeriod(Duration period) {
            this.period = period;
            return this;
        }

        public TestTaskBuilder withHandlingDuration(Duration handlingDuration) {
            this.handlingDuration = handlingDuration;
            return this;
        }

        public TestTask build() {
            TestTask testTask = new TestTask();
            testTask.setHandlingDuration(handlingDuration);
            testTask.setMaximumUncompletedExecutionsOnNode(this.maximumUncompletedExecutionsOnNode);
            testTask.setClaimedSignalPeriod(this.claimedSignalPeriod);
            testTask.setResurrectionInterval(this.resurrectionInterval);
            testTask.setInitialTimestamp(this.initialTimestamp);
            testTask.period = this.period;
            testTask.setMaxExecutionsOnNode(this.maxExecutionsOnNode);
            testTask.name = this.name;
            testTask.setMaxDuration(this.maxDuration);
            return testTask;
        }
    }
}
