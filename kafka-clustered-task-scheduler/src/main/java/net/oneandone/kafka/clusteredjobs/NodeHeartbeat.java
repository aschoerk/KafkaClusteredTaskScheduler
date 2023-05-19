package net.oneandone.kafka.clusteredjobs;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefaults;

/**
 * @author aschoerk
 */
public class NodeHeartbeat extends TaskDefaults {


    private Duration period = Duration.ofSeconds(5);
    private String name = "NodeHeartbeat";

    NodeHeartbeat(final Duration period) {
        this.period = period;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Duration getPeriod() {
        return period;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Runnable getCode(final Node node) {
        return new Runnable() {
            @Override
            public void run() {
                NodeImpl nodeImpl = (NodeImpl) node;
                nodeImpl.sendNodeTaskInformation(true);
                ((NodeImpl) node).getPendingHandler().scheduleNodeHeartBeat(this, node.getNow().plus(getPeriod()));
            }
        };
    }

}
