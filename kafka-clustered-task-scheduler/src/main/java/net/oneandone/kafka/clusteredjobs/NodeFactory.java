package net.oneandone.kafka.clusteredjobs;

import java.time.Duration;
import java.time.Instant;

import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;

/**
 * Interface of Factory for Subcomponents of Nodes
 */
public interface NodeFactory {

    /**
     * create SignalsWatcher-Object
     * @param node the node the object is watching the signals for
     * @return a new SignalsWatcher
     */
    SignalsWatcher createSignalsWatcher(NodeImpl node);

    /**
     * create a Signal-Handler Object
     * @param node the node the object is watching the signals for
     * @return a new SignalHandler
     */
    SignalHandler createSignalHandler(NodeImpl node);

    /**
     * create a NodeHeartbeat Object
     * @param period of the Heartbeat
     * @return a new SignalHandler
     */
    NodeHeartbeat createNodeHeartbeat(Duration period);

    /**
     * create PendingHandler-Object
     * @param node the node the object is handling pending tasks for
     * @return a new PendingHandler
     */
    PendingHandler createPendingHandler(NodeImpl node);
    /**
     * create Sender-Object capable of sending tasks to the syncTopic
     * @param node the node the sender is sending the objects for
     * @return a new Sender object capable of sending tasks to the syncTopic
     */
    Sender createSender(NodeImpl node);

    /**
     * create NodeTaskInformationHandler-Object
     * @param node the node the object interpreting NodeTaskInformations for
     * @return a new NodeTaskInformationHandler
     */
    NodeTaskInformationHandler createNodeTaskInformationHandler(NodeImpl node);

    /**
     * create a new PendingEntry for the PendingHandler
     * @param timestamp the timestamp the runnable should be started
     * @param name the unique name of the pending entry. There can only be one Entry with a certain name be pending
     * @param runnable the runnable to be executed when timestamp elapsd
     * @return the new PendingENtry
     */
    PendingEntry createPendingEntry(Instant timestamp, String name, Runnable runnable);

    /**
     * create a new TaskImpl for node
     * @param node the node the task is created for
     * @param taskDefinition the definition, how the task is to be scheduled, controlled, executed
     * @return the new runtime-representation of a TaskImpl on this node.
     */
    TaskImpl createTask(Node node, TaskDefinition taskDefinition);

}
