package net.oneandone.kafka.clusteredjobs.api;

import java.time.Instant;

public interface Node {
    String getUniqueNodeId();
    Task register(TaskDefinition taskDefinition);
    Task getTask(String taskName);
    Instant getNow();
    Container getContainer();
    NodeInformation getNodeInformation();
}
