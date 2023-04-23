package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.CLAIMED_BY_ME;
import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.HANDLED_BY_ME;

/**
 * @author aschoerk
 */
public class TaskHandler extends StoppableBase {
    Task t;
    Sender sender;

    public TaskHandler(final Task t, final Sender sender) {
        this.t = t;
        this.sender = sender;
    }

    public void run() {
        initThreadName("TaskHandler");
        sender.sendSynchronous(t, HANDLED_BY_ME);
        t.getJob().run();
        t.setLocalState(TaskStateEnum.CLAIMED_BY_NODE);
        sender.sendSynchronous(t, CLAIMED_BY_ME);
    }
}
