package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import net.oneandone.kafka.clusteredjobs.api.NodeInformation;
import net.oneandone.kafka.clusteredjobs.api.TaskStateEnum;

/**
 * @author aschoerk
 */
public class NodeInformationImpl implements NodeInformation {

    public NodeInformationImpl(final String name) {
        this.name = name;
    }

    private String name;

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

    public void setOffset(final Long offset) {
        this.offset = offset;
    }

    public void setArrivalTime(final Instant arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    static class TaskInformationImpl implements TaskInformation {

        private String name;
        private TaskStateEnum tastState;
        private String nodeName;

        public TaskInformationImpl(final String name, final TaskStateEnum tastState, final String nodeName) {
            this.name = name;
            this.tastState = tastState;
            this.nodeName = nodeName;
        }

        public TaskInformationImpl(Task task) {
            this.name = task.getDefinition().getName();
            this.tastState = task.getLocalState();
            this.nodeName = task.getCurrentExecutor().orElse(null);
        }

        @Override
        public String getTaskName() {
            return name;
        }

        @Override
        public TaskStateEnum getState() {
            return tastState;
        }

        @Override
        public Optional<String> getNodeName() {
            return Optional.ofNullable(nodeName);
        }

        @Override
        public String toString() {
            return "TaskInformationImpl{" +
                   "name='" + name + '\'' +
                   ", tastState=" + tastState +
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
