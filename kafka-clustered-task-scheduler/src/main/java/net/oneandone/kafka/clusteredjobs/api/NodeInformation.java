package net.oneandone.kafka.clusteredjobs.api;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * @author aschoerk
 */
public interface NodeInformation {
    String getName();

    Optional<Long> getOffset();

    Optional<Instant> getArrivalTime();

    interface TaskInformation {
        String getTaskName();
        TaskStateEnum getState();
        Optional<String> getNodeName();
    }

    List<TaskInformation> getTaskInformation();
}
