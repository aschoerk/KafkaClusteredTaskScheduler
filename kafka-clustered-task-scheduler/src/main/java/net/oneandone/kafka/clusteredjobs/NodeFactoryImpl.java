package net.oneandone.kafka.clusteredjobs;

import java.time.Duration;
import java.time.Instant;

import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;

/**
 * @author aschoerk
 */
public class NodeFactoryImpl implements NodeFactory {
    @Override
    public SignalsWatcher createSignalsWatcher(final NodeImpl node) {
        return new SignalsWatcher(node);
    }

    @Override
    public SignalHandler createSignalHandler(final NodeImpl node) {
        return new SignalHandler(node);
    }

    @Override
    public NodeHeartbeat createNodeHeartbeat(final Duration period) {
        return new NodeHeartbeat(period);
    }

    @Override
    public PendingHandler createPendingHandler(final NodeImpl node) {
        return new PendingHandler(node);
    }

    @Override
    public Sender createSender(final NodeImpl node) {
        return new Sender(node);
    }

    @Override
    public NodeTaskInformationHandler createNodeTaskInformationHandler(final NodeImpl node) {
        return new NodeTaskInformationHandler(node);
    }

    @Override
    public PendingEntry createPendingEntry(final Instant timestamp, final String name, final Runnable runnable) {
        return new PendingEntry(timestamp, name, runnable);
    }

    @Override
    public TaskImpl createTask(final Node node, final TaskDefinition taskDefinition) {
        return new TaskImpl(node, taskDefinition);
    }
}
