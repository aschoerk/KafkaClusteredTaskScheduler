package net.oneandone.kafka.clusteredjobs;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.clients.producer.ProducerRecord;

import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefaults;

/**
 * @author aschoerk
 */
public class NodeHeartbeat extends TaskDefaults {

    private Duration period;
    private String name;
    private Runnable job;

    private NodeHeartbeat(final String name, final Duration period, final Runnable job) {
        this.name = name;
        this.period = period;
        this.job = job;
    }

    @Override
    public Duration getPeriod() {
        if (period == null) {
            period = Duration.ofSeconds(5);
        }
        return period;
    }

    @Override
    public String getName() {
        if (name == null) {
            name = "NodeHeartbeat";
        }
        return name;
    }

    @Override
    public Runnable getJob(final Node node) {
        return new Runnable() {
            @Override
            public void run() {
                NodeImpl nodeImpl = (NodeImpl) node;
                nodeImpl.sendHeartbeat();
                ((NodeImpl) node).getPendingHandler().scheduleNodeHeartBeat(this, node.getNow().plus(getPeriod()));
            }
        };
    }

    public static final class NodeHeartBeatBuilder {
        private Duration period;
        private String name;
        private Instant initialTimestamp;
        private Duration maxDuration;
        private int maximumUncompletedExecutionsOnNode;
        private Duration claimedSignalPeriod;
        private Long maxExecutionsOnNode;
        private Duration resurrectionInterval;

        private NodeHeartBeatBuilder() {}

        public static NodeHeartBeatBuilder aNodeHeartBeat() {return new NodeHeartBeatBuilder();}

        public NodeHeartBeatBuilder withPeriod(Duration period) {
            this.period = period;
            return this;
        }

        public NodeHeartBeatBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public NodeHeartBeatBuilder withInitialTimestamp(Instant initialTimestamp) {
            this.initialTimestamp = initialTimestamp;
            return this;
        }

        public NodeHeartBeatBuilder withMaxDuration(Duration maxDuration) {
            this.maxDuration = maxDuration;
            return this;
        }

        public NodeHeartBeatBuilder withMaximumUncompletedExecutionsOnNode(int maximumUncompletedExecutionsOnNode) {
            this.maximumUncompletedExecutionsOnNode = maximumUncompletedExecutionsOnNode;
            return this;
        }

        public NodeHeartBeatBuilder withClaimedSignalPeriod(Duration claimedSignalPeriod) {
            this.claimedSignalPeriod = claimedSignalPeriod;
            return this;
        }

        public NodeHeartBeatBuilder withMaxExecutionsOnNode(Long maxExecutionsOnNode) {
            this.maxExecutionsOnNode = maxExecutionsOnNode;
            return this;
        }

        public NodeHeartBeatBuilder withResurrectionInterval(Duration resurrectionInterval) {
            this.resurrectionInterval = resurrectionInterval;
            return this;
        }

        public NodeHeartbeat build() {
            NodeHeartbeat nodeHeartBeat = new NodeHeartbeat(name, period, null);
            nodeHeartBeat.maximumUncompletedExecutionsOnNode = this.maximumUncompletedExecutionsOnNode;
            nodeHeartBeat.claimedSignalPeriod = this.claimedSignalPeriod;
            nodeHeartBeat.resurrectionInterval = this.resurrectionInterval;
            nodeHeartBeat.initialTimestamp = this.initialTimestamp;
            nodeHeartBeat.maxExecutionsOnNode = this.maxExecutionsOnNode;
            nodeHeartBeat.maxDuration = this.maxDuration;
            return nodeHeartBeat;
        }
    }
}
