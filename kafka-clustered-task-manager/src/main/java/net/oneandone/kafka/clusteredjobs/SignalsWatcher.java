package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.Node.CONSUMER_POLL_TIME;
import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.CLAIMED_BY_ME;
import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.HANDLED_BY_ME;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
public class SignalsWatcher extends StoppableBase {
    Logger logger = LoggerFactory.getLogger(Node.class);
    private Node node;
    private Sender sender;

    ConcurrentHashMap<NodesTaskSignal, NodesTaskSignal> lastSignalFromNode = new ConcurrentHashMap<>();

    ConcurrentHashMap<String, ConcurrentHashMap<String, NodesTaskSignal>> signalsPerTask = new ConcurrentHashMap<>();


    public SignalsWatcher(Node node, Sender sender) {
        this.node = node;
        this.sender = sender;
    }

    public void run() {
        initThreadName(this.getClass().getSimpleName());
        Map<String, Object> syncingConsumerConfig = new HashMap<>();
        syncingConsumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, node.bootstrapServers);
        syncingConsumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        syncingConsumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        syncingConsumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1);
        syncingConsumerConfig.put(MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        syncingConsumerConfig.put(GROUP_ID_CONFIG, node.getUniqueNodeId());
        syncingConsumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        // This is required, otherwise kafka would automatically commit messages, even in the event
        // of exceptions while publishing to mobile event hub. This would cause messages to be lost!
        // (the setting may be disabled using the external configuration)
        try (KafkaConsumer consumer = new KafkaConsumer<>(syncingConsumerConfig)) {
            consumer.subscribe(Arrays.asList(node.syncTopic));
            ConsumerRecords<String, String> records;
            while (!doShutdown()) {
                records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIME));
                if (records.count() > 0) {
                    logger.debug("Node: {} found {} records", node.getUniqueNodeId(), records.count());
                    records.forEach(r -> {
                        if(node.taskPartition == null) {
                            node.taskPartition = r.partition();
                        }
                        else {
                            if(!node.taskPartition.equals(r.partition())) {
                                logger.error("TaskManager may only synchronize via single-Partition Topic.");
                                throw new RuntimeException("TaskManager may only synchronize via single-Partition Topic.");
                            }
                        }
                        Object event = (NodesTaskSignal) KbXStream.jsonXStream.fromXML(r.value());
                        if(event instanceof NodesTaskSignal) {
                            NodesTaskSignal nodesTaskSignal = (NodesTaskSignal) event;
                            nodesTaskSignal.currentOffset = r.offset();
                            Task task = node.tasks.get(((NodesTaskSignal) event).taskName);
                            logger.debug("Record_Offset: {} Task: {} Received Signal {}", r.offset(), task.getName(), nodesTaskSignal);
                            if(task != null) {
                                lastSignalFromNode.put(nodesTaskSignal, nodesTaskSignal);
                                if(signalsPerTask.get(nodesTaskSignal.taskName) == null) {
                                    signalsPerTask.put(nodesTaskSignal.taskName, new ConcurrentHashMap<>());
                                }
                                signalsPerTask.get(nodesTaskSignal.taskName).put(nodesTaskSignal.nodeProcThreadId, nodesTaskSignal);
                            }
                            else {
                                logger.warn("Received Signal from unknown task {}", nodesTaskSignal);
                            }
                        }
                        else {
                            // ignore
                        }
                    });
                    signalsPerTask.entrySet().forEach(e -> {
                        ConcurrentHashMap<String, NodesTaskSignal> taskInfo = e.getValue();
                        checkTaskState(e.getKey(), taskInfo);
                    });

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        logger.info("Running for node: {} interrupted", node.getUniqueNodeId() );
                        return;
                    }
                }
            }
        }
    }

    private void checkTaskState(String taskName, final ConcurrentHashMap<String, NodesTaskSignal> taskInfo) {
        // is task claimed?
        final TaskStateEnum localState = node.tasks.get(taskName).getLocalState();

        Map<TaskSignalEnum, List<NodesTaskSignal>> relevantTaskInfos = taskInfo.values().stream()
                .filter(ti -> ti.timestamp.isAfter(Instant.now().minus(node.MAX_AGE_OF_SIGNAL, ChronoUnit.MILLIS)))
                .collect(Collectors.groupingBy(nodesTaskSignal -> nodesTaskSignal.signal));

        Arrays.stream(TaskSignalEnum.values()).forEach(k -> {
            if(!relevantTaskInfos.containsKey(k)) relevantTaskInfos.put(k, Collections.emptyList());
        });

        if(relevantTaskInfos.get(HANDLED_BY_ME).size() > 1) {
            logger.error(relevantTaskInfos.get(HANDLED_BY_ME).stream().map(s -> s.nodeProcThreadId).reduce("Multiple handlers: ",
                    (s, s2) -> s += ";" + s2));
        }

        List<NodesTaskSignal> nodesWhichClaimed = Stream.concat(relevantTaskInfos.get(CLAIMED_BY_ME).stream(), relevantTaskInfos.get(HANDLED_BY_ME).stream()).collect(Collectors.toList());

        if(nodesWhichClaimed.size() == 1) {
            node.tasks.get(taskName).sawClaimedInfo();
            final NodesTaskSignal claimedSignal = nodesWhichClaimed.get(0);
            if(localState == TaskStateEnum.CLAIMED_BY_NODE || localState == TaskStateEnum.HANDLING_BY_NODE) {
                if(!claimedSignal.nodeProcThreadId.equals(node.getUniqueNodeId())) {
                    logger.error("I {} thought to have claimed task {} but claimed by {} removing it.", node.getUniqueNodeId(), claimedSignal.taskName, claimedSignal.nodeProcThreadId);
                    node.tasks.get(claimedSignal.taskName).setLocalState(TaskStateEnum.UNCLAIM);
                    return;
                }
            }
            else if(claimedSignal.nodeProcThreadId.equals(node.getUniqueNodeId())) {
                logger.error("I {} received signal to have claimed task {} but is not there.", node.getUniqueNodeId(), claimedSignal.taskName, claimedSignal.nodeProcThreadId);
            }
        }
        else if(nodesWhichClaimed.size() == 0) {
            logger.info("Nobody claimed task {}", taskName);
        }
        else if(nodesWhichClaimed.size() > 1) {
            node.tasks.get(taskName).sawClaimedInfo();
            logger.error("Task {} claimed by multiple nodes {}", taskName, nodesWhichClaimed.stream().map(ti -> ti.nodeProcThreadId).collect(Collectors.joining(",")));
        }
        List<NodesTaskSignal> claimingNodes = relevantTaskInfos.get(TaskSignalEnum.CLAIMING);
        if(claimingNodes.size() > 0 && (localState == TaskStateEnum.CLAIMING || localState == TaskStateEnum.INITIATING)) {
            claimingNodes.sort((t, t2) -> ((Long) (t.currentOffset)).compareTo(t2.currentOffset));
            // if this node was the first to have sent CLAIMING
            if(claimingNodes.get(0).nodeProcThreadId.equals(node.getUniqueNodeId())) {
                final Task task = node.tasks.get(taskName);
                task.setLocalState(TaskStateEnum.CLAIMED_BY_NODE);
                sender.sendAsynchronous(task, CLAIMED_BY_ME);
            }
            else {
                node.tasks.get(taskName).setLocalState(TaskStateEnum.CLAIMED_BY_OTHER);
            }
        }
    }
}
