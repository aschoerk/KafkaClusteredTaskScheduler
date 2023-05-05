package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.Node.CONSUMER_POLL_TIME;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
    ConcurrentHashMap<String, ConcurrentHashMap<String, Signal>> lastSignalPerTaskAndNode = new ConcurrentHashMap<>();
    // no need to be threadsafe yet


    public SignalsWatcher(Node node) {
        this.node = node;
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
                        if(!(event instanceof Signal)) {
                            throw new KctmException("Expected only Signal Objects in sync-topic but was: " + event.getClass().getName());
                        }
                        final Signal signal = (Signal) event;
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


    static class OffsetContainer {
        Set<Long> offsetSet1 = new HashSet<>();
        Set<Long> offsetSet2 = new HashSet<>();
        boolean useSetOne = true;
    }

    public void run() {
        initThreadName(this.getClass().getSimpleName());

        final OffsetContainer oC = new OffsetContainer();


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getSyncingConsumerConfig(node))) {
            Pair<TopicPartition, Long> startOffset = findConsumerPosition(getSyncingConsumerConfig(node), SEARCH_SYNC_OFFSET_TRIGGER);
            consumer.assign(Collections.singletonList(startOffset.getLeft()));
            consumer.seek(startOffset.getLeft(), startOffset.getRight());
            ConsumerRecords<String, String> records;
            while (!doShutdown()) {
                records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIME));
                if(records.count() > 0) {
                    logger.debug("N: {} found {} records", node.getUniqueNodeId(), records.count());
                    records.forEach(r -> {
                        if (oC.offsetSet1.contains(r.offset()) || oC.offsetSet2.contains(r.offset())) {
                            logger.error("Repeated offset: {}");
                        } else {
                            if (oC.useSetOne) {
                                oC.offsetSet1.add(r.offset());
                                if (oC.offsetSet1.size() > 100) {
                                    oC.useSetOne = false;
                                    oC.offsetSet2.clear();
                                }
                            } else {
                                oC.offsetSet2.add(r.offset());
                                if (oC.offsetSet2.size() > 100) {
                                    oC.useSetOne = true;
                                    oC.offsetSet1.clear();
                                }
                            }
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
                            if(event instanceof Signal) {
                                Signal signal = (Signal) event;
                                signal.setCurrentOffset(r.offset());
                                Task task = node.tasks.get(signal.taskName);
                                logger.debug("N: {} Offs: {} T: {}/{} S: {}/{}", node.getUniqueNodeId(), r.offset(), task.getName(), task.getLocalState(), signal.nodeProcThreadId, signal.signal);
                                if(task != null) {
                                    if(lastSignalPerTaskAndNode.get(signal.taskName) == null) {
                                        lastSignalPerTaskAndNode.put(signal.taskName, new ConcurrentHashMap<>());
                                    }
                                    Signal previousSignalFromThisNodeForThisTask = lastSignalPerTaskAndNode.get(signal.taskName).get(signal.nodeProcThreadId);
                                    if(previousSignalFromThisNodeForThisTask != null) {
                                        if(previousSignalFromThisNodeForThisTask.getCurrentOffset().get() > signal.getCurrentOffset().get()) {
                                            logger.warn("Handled older signal {} for Task later found: {}", signal, previousSignalFromThisNodeForThisTask);
                                        }
                                    }
                                    lastSignalPerTaskAndNode.get(signal.taskName).put(signal.nodeProcThreadId, signal);
                                }
                                else {
                                    logger.warn("Received Signal from unknown task {}", signal);
                                }
                            }
                            else {
                                throw new KctmException("Unexpected event of type: " + event.getClass().getName() + " on synctopic");
                            }
                        }
                    });
                    logger.info("Received {} signals",lastSignalPerTaskAndNode.entrySet().size());
                    lastSignalPerTaskAndNode.entrySet().forEach(e -> {
                        node.getSignalHandler().handle(e.getKey(), e.getValue());
                    });
                    lastSignalPerTaskAndNode.clear();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        if (!doShutdown())
                            logger.error("SignalsWatcher N: got interrupted {}",node.getUniqueNodeId(), e);
                        else
                            logger.info("SignalsWatcher N: got interrupted {}",node.getUniqueNodeId(), e);
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

}
