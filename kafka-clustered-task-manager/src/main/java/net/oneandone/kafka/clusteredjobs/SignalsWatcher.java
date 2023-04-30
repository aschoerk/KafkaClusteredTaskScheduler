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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
public class SignalsWatcher extends StoppableBase {
    private static final long SEARCH_SYNC_OFFSET_TRIGGER = 1000;
    Logger logger = LoggerFactory.getLogger(Node.class);
    private Node node;
    private Sender sender;

    ConcurrentHashMap<String, ConcurrentHashMap<String, NodesTaskSignal>> lastSignalPerTaskAndNode = new ConcurrentHashMap<>();


    public SignalsWatcher(Node node, Sender sender) {
        this.node = node;
        this.sender = sender;
    }

    Pair<TopicPartition, Long> findConsumerPosition(Map<String, Object> consumerConfig, long searchSyncOffsetTrigger) {
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            List<PartitionInfo> partitionInfo = consumer.listTopics().get(node.syncTopic);
            if(partitionInfo != null && partitionInfo.size() != 1) {
                throw new KctmException("Either topic " + node.syncTopic + " not found or more than one partition defined");
            }
            else {
                PartitionInfo thePartitionInfo = partitionInfo.get(0);
                TopicPartition partition = new TopicPartition(node.syncTopic, thePartitionInfo.partition());
                consumer.assign(Collections.singletonList(partition));
                // consumer.subscribe(Collections.singletonList(node.syncTopic));
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singletonList(partition), Duration.ofSeconds(2));
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(partition), Duration.ofSeconds(2));
                if(endOffsets.size() != 1 && beginningOffsets.size() != 1) {
                    throw new KctmException("Could not get information about sync Topic in time");
                }
                long endOffset = endOffsets.get(partition).longValue();
                long beginningOffset = beginningOffsets.get(partition).longValue();
                if((endOffset - beginningOffset) > searchSyncOffsetTrigger) {
                    boolean lowerOffsetFound = false;
                    while (!lowerOffsetFound) {
                        long medium = beginningOffset + (endOffset - beginningOffset) / 2L;
                        consumer.seek(partition, medium);
                        ConsumerRecords<String, String> records = consumer.poll(1000);
                        if(records.count() > 1) {
                            throw new KctmException("Normally only one record should get returned using this config");
                        }
                        else if(records.count() == 0) {
                            logger.warn("Expected record to be returned, repeat consume");
                            continue;
                        }
                        final ConsumerRecord<String, String> record = records.iterator().next();
                        Object event = KbXStream.jsonXStream.fromXML(record.value());
                        if(!(event instanceof NodesTaskSignal)) {
                            throw new KctmException("Expected only NodesTaskSignal Objects in sync-topic but was: " + event.getClass().getName());
                        }
                        final NodesTaskSignal signal = (NodesTaskSignal) event;
                        if(signal.timestamp.isAfter(getNow().minus(node.MAX_AGE_OF_SIGNAL, ChronoUnit.MILLIS))) {
                            endOffset = medium;
                        }
                        else {
                            beginningOffset = medium;
                        }
                        lowerOffsetFound = endOffset - beginningOffset <= 1;
                    }
                    return Pair.of(partition, beginningOffset);
                }
                return Pair.of(partition, beginningOffset);
            }
        }
        // get the earliest and latest offsets for the partition


    }

    private Instant getNow() {
        return Instant.now(node.getClock());
    }

    public void run() {
        initThreadName(this.getClass().getSimpleName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getSyncingConsumerConfig(node))) {
            consumer.subscribe(Arrays.asList(node.syncTopic));
            Pair<TopicPartition, Long> startOffset = findConsumerPosition(getSyncingConsumerConfig(node), SEARCH_SYNC_OFFSET_TRIGGER);
            consumer.seek(startOffset.getLeft(), startOffset.getRight());
            ConsumerRecords<String, String> records;
            while (!doShutdown()) {
                records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIME));
                if(records.count() > 0) {
                    logger.debug("Node: {} found {} records", node.getUniqueNodeId(), records.count());
                    records.forEach(r -> {
                        if(node.taskPartition == null) {
                            node.taskPartition = r.partition();
                        }
                        else {
                            if(!node.taskPartition.equals(r.partition())) {
                                logger.error("TaskManager may only synchronize via single-Partition Topic.");
                                throw new KctmException("TaskManager may only synchronize via single-Partition Topic.");
                            }
                        }
                        Object event = KbXStream.jsonXStream.fromXML(r.value());
                        if(event instanceof NodesTaskSignal) {
                            NodesTaskSignal nodesTaskSignal = (NodesTaskSignal) event;
                            nodesTaskSignal.setCurrentOffset(r.offset());
                            Task task = node.tasks.get(nodesTaskSignal.taskName);
                            logger.debug("Record_Offset: {} Task: {} Received Signal {}", r.offset(), task.getName(), nodesTaskSignal);
                            if(task != null) {
                                if(lastSignalPerTaskAndNode.get(nodesTaskSignal.taskName) == null) {
                                    lastSignalPerTaskAndNode.put(nodesTaskSignal.taskName, new ConcurrentHashMap<>());
                                }
                                NodesTaskSignal previousSignalFromThisNodeForThisTask = lastSignalPerTaskAndNode.get(nodesTaskSignal.taskName).get(nodesTaskSignal.nodeProcThreadId);
                                if (previousSignalFromThisNodeForThisTask != null) {
                                    if (previousSignalFromThisNodeForThisTask.getCurrentOffset().get() > nodesTaskSignal.getCurrentOffset().get() ) {
                                        logger.warn("Handled older signal {} for Task later found: {}", nodesTaskSignal, previousSignalFromThisNodeForThisTask );
                                    }
                                }
                                lastSignalPerTaskAndNode.get(nodesTaskSignal.taskName).put(nodesTaskSignal.nodeProcThreadId, nodesTaskSignal);
                            }
                            else {
                                logger.warn("Received Signal from unknown task {}", nodesTaskSignal);
                            }
                        }
                        else {
                            throw new KctmException("Unexpected event of type: " + event.getClass().getName() + " on synctopic");
                        }
                    });
                    lastSignalPerTaskAndNode.entrySet().forEach(e -> {
                        ConcurrentHashMap<String, NodesTaskSignal> taskInfo = e.getValue();
                        checkTaskState(e.getKey(), taskInfo);
                    });

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        logger.info("Running for node: {} interrupted", node.getUniqueNodeId());
                        return;
                    }
                }
            }
        }
    }

    static Map<String, Object> getSyncingConsumerConfig(final Node nodeP) {
        Map<String, Object> syncingConsumerConfig = new HashMap<>();
        syncingConsumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, nodeP.bootstrapServers);
        syncingConsumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        syncingConsumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        syncingConsumerConfig.put(MAX_POLL_RECORDS_CONFIG, 100);
        syncingConsumerConfig.put(MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        syncingConsumerConfig.put(GROUP_ID_CONFIG, nodeP.getUniqueNodeId());
        syncingConsumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return syncingConsumerConfig;
    }

    private void checkTaskState(String taskName, final ConcurrentHashMap<String, NodesTaskSignal> taskInfo) {
        // is task claimed?
        final TaskStateEnum localState = node.tasks.get(taskName).getLocalState();

        Map<TaskSignalEnum, List<NodesTaskSignal>> relevantTaskInfos = taskInfo.values().stream()
                .filter(ti -> ti.timestamp.isAfter(Instant.now(node.getClock()).minus(node.MAX_AGE_OF_SIGNAL, ChronoUnit.MILLIS)))
                .filter(ti -> !ti.isHandled())
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
            claimingNodes.sort((t1, t2) -> t1.before(t2) ? -1 : !t2.before(t1) ? 0 : 1);
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
