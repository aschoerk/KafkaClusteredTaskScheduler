package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import net.oneandone.kafka.clusteredjobs.api.NodeTaskInformation;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * @author aschoerk
 */
class NodeTaskInformationImpl implements NodeTaskInformation {

    NodeTaskInformationImpl(final String name) {
        this.name = name;
    }

    private final String name;

    private transient Long offset = null;

    private transient Instant arrivalTime = null;

    ArrayList<TaskInformation> tasks = new ArrayList<>();
    @Override
    public String getName() {
        return name;
    }

    @Override
    public Optional<Long> getOffset() {
        return Optional.ofNullable(offset);
    }

    @Override
    public Optional<Instant> getArrivalTime() {
        return Optional.ofNullable(arrivalTime);
    }

    /**
     * in case of the object arrived as event, the offset where the event must be set using this
     * @param offset the offset in the one kafka-partition of the topic
     */
    public void setOffset(final Long offset) {
        this.offset = offset;
    }

    /**
     * set the time the event arrived.
     * @param arrivalTime the time the event arrived
     */
    public void setArrivalTime(final Instant arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    static class TaskInformationImpl implements TaskInformation {

        private final String name;
        private final StateEnum taskState;
        private final String nodeName;

        public TaskInformationImpl(final String name, final StateEnum taskState, final String nodeName) {
            this.name = name;
            this.taskState = taskState;
            this.nodeName = nodeName;
        }

        public TaskInformationImpl(TaskImpl task) {
            this.name = task.getDefinition().getName();
            this.taskState = task.getLocalState();
            this.nodeName = task.getCurrentExecutor().orElse(null);
        }

        @Override
        public String getTaskName() {
            return name;
        }

        @Override
        public StateEnum getState() {
            return taskState;
        }

        @Override
        public Optional<String> getNodeName() {
            return Optional.ofNullable(nodeName);
        }

        @Override
        public String toString() {
            return "TaskInformationImpl{" +
                   "name='" + name + '\'' +
                   ", tastState=" + taskState +
                   ", nodeName='" + nodeName + '\'' +
                   '}';
        }
    }

    @Override
    public List<TaskInformation> getTaskInformation() {
        return tasks;
    }

    void addTaskInformation(TaskInformation taskInformation) {
        tasks.add(taskInformation);
    }
}
