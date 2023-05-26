package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.NodeImpl.CONSUMER_POLL_TIME;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.NodeTaskInformation;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;


/**
 * @author aschoerk
 */
public class SignalsWatcher extends StoppableBase {
    private static final long SEARCH_SYNC_OFFSET_TRIGGER = 1000;
    Logger logger = LoggerFactory.getLogger(NodeImpl.class);
    private NodeImpl node;
    ConcurrentHashMap<String, ConcurrentHashMap<String, Signal>> lastSignalPerTaskAndNode = new ConcurrentHashMap<>();

    transient volatile ConsumerData watcherStarting;
    // no need to be threadsafe yet


    /**
     * The partition information before the first event gets consumed.
     * @return the partition information before the first event gets consumed.
     */
    public ConsumerData getWatcherStarting() {
        return watcherStarting;
    }

    /**
     * create the SignalsWatcher for node
     * @param node the node the SignalsWatcher is destined for.
     */
    public SignalsWatcher(NodeImpl node) {
        this.node = node;
    }


    static class OffsetContainer {
        Set<Long> offsetSet1 = new HashSet<>();
        Set<Long> offsetSet2 = new HashSet<>();
        boolean useSetOne = true;
    }

    static NodeTaskInformation nullNodeTaskInformation = new NodeTaskInformationImpl("Empty");

    Map<String, NodeTaskInformation> lastNodeInformation = new ConcurrentHashMap<>();

    private ArrayList<Signal> oldSignals = new ArrayList<>();

    private ArrayList<Signal> unmatchedSignals = new ArrayList();

    void readOldSignals() {
        iterateOldRecords(node.syncTopic, node.bootstrapServers, node.getUniqueNodeId(), r -> {
            if(r.value().contains("ignal")) {
                Signal signal = JsonMarshaller.gson.fromJson(r.value(), Signal.class);
                signal.setCurrentOffset(r.offset());
                oldSignals.add(signal);
            }
            else {
                NodeTaskInformationImpl nodeInformation =
                        JsonMarshaller.gson.fromJson(r.value(), NodeTaskInformationImpl.class);
                nodeInformation.setOffset(r.offset());
                NodeTaskInformation existing = lastNodeInformation.get(nodeInformation.getName());
                if(existing == null || existing.getOffset().get() < nodeInformation.getOffset().get()) {
                    lastNodeInformation.put(nodeInformation.getName(), nodeInformation);
                }
            }
        });
        identifyOldNodeInformation();
    }

    static void iterateOldRecords(String topic, String bootstrapServers, String groupId, Consumer<ConsumerRecord<String, String>> eventHandler) {
        Map<String, Object> consumerConfig = getSyncingConsumerConfig(bootstrapServers, groupId);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 100);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            ConsumerData consumerData = initConsumer(topic, consumer);
            long endOffset = consumerData.endOffset;
            AtomicBoolean done = new AtomicBoolean(false);
            while (!done.get() && endOffset >= consumerData.startOffset) {
                long startoffset = endOffset - 100;
                if(startoffset < consumerData.startOffset) {
                    startoffset = consumerData.startOffset;
                }
                long offset = startoffset;
                do {
                    consumer.seek(consumerData.partition, offset);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> r: records) {
                        if (r.offset() <= endOffset) {
                            eventHandler.accept(r);
                        }
                    }
                    offset += records.count();
                } while (offset < endOffset);
                endOffset = startoffset - 1;
            }
        }
    }

    private void identifyOldNodeInformation() {
        // principle: remove NodeTaskInformation which contradicts with more recent information
        HashMap<String, NodeTaskInformation> reducedNodeInformation = lastNodeInformation
                .entrySet()
                .stream()
                .filter(e -> e.getValue().getName().equals(e.getKey()))
                .map(e -> e.getValue())
                .sorted((e1, e2) -> -e1.getOffset().get().compareTo(e2.getOffset().get()))
                .reduce(new HashMap<String, NodeTaskInformation>(),
                        (result, e) -> {
                            e.getTaskInformation().stream().forEach(ti -> {
                                if((ti.getState() == StateEnum.CLAIMED_BY_NODE || ti.getState() == StateEnum.HANDLING_BY_NODE)
                                   && !result.containsKey(ti.getTaskName())) {
                                    result.put(ti.getTaskName(), e);
                                }
                            });
                            return result;
                        }, (a, b) -> null)
                .entrySet()
                .stream()
                .reduce(new HashMap<String, NodeTaskInformation>(), (result, e) -> {
                    result.put(e.getValue().getName(), e.getValue());
                    return result;
                }, (a, b) -> null);
        lastNodeInformation.keySet().forEach(k -> {
            if (lastNodeInformation.get(k).getTaskInformation().size() > 0 && !reducedNodeInformation.containsKey(k))
                lastNodeInformation.put(k, nullNodeTaskInformation);
        });
        node.getNodeTaskInformationHandler().setSignalsWatcher(this);
    }

    public void run() {
        initThreadName(this.getClass().getSimpleName());

        final OffsetContainer oC = new OffsetContainer();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getSyncingConsumerConfig(node.bootstrapServers, node.getUniqueNodeId()))) {
            setRunning();
            synchronized (node) {
                watcherStarting = initConsumer(node.syncTopic, consumer);
                logger.info("Going to notify about SignalWatcher Topic consumer init.");
                node.notify();
                logger.info("End   of notify about SignalWatcher Topic consumer init.");
            }
            ConsumerRecords<String, String> records;
            while (!doShutdown()) {
                records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIME));
                if(records.count() > 0) {
                    logger.debug("N: {} found {} records", node.getUniqueNodeId(), records.count());
                    records.forEach(r -> {
                        if(oC.offsetSet1.contains(r.offset()) || oC.offsetSet2.contains(r.offset())) {
                            logger.error("Repeated offset");
                        }
                        else {
                            if(oC.useSetOne) {
                                oC.offsetSet1.add(r.offset());
                                if(oC.offsetSet1.size() > 100) {
                                    oC.useSetOne = false;
                                    oC.offsetSet2.clear();
                                }
                            }
                            else {
                                oC.offsetSet2.add(r.offset());
                                if(oC.offsetSet2.size() > 100) {
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
                                    throw new KctmException("TaskScheduler may only synchronize via single-Partition Topic.");
                                }
                            }
                            if(!r.value().startsWith("{\"name\":")) {
                                Object event = JsonMarshaller.gson.fromJson(r.value(), Signal.class);
                                Signal signal = (Signal) event;
                                signal.setCurrentOffset(r.offset());
                                TaskImpl task = node.tasks.get(signal.taskName);

                                if(task == null && signal.signal == SignalEnum.DOHEARTBEAT) {
                                    if (!signal.equalNode(node)) {
                                        logger.debug("Node: {} triggering NodeHeartBeat beause of DOHEARTBEAT", node.getUniqueNodeId());
                                        node.getPendingHandler().scheduleNodeHeartBeat(node.getNodeHeartbeat().getCode(node), node.getNow());
                                    }
                                }
                                else if(task != null) {
                                    logger.debug("N: {} Offs: {} T: {}/{} S: {}/{}", node.getUniqueNodeId(), r.offset(),
                                            signal.taskName, task.getLocalState(), signal.nodeProcThreadId, signal.signal);
                                    if(lastSignalPerTaskAndNode.get(signal.taskName) == null) {
                                        lastSignalPerTaskAndNode.put(signal.taskName, new ConcurrentHashMap<>());
                                    }
                                    Signal previousSignalFromThisNodeForThisTask = lastSignalPerTaskAndNode.get(signal.taskName).get(signal.nodeProcThreadId);
                                    if(previousSignalFromThisNodeForThisTask != null) {
                                        if(previousSignalFromThisNodeForThisTask.getCurrentOffset().get() > signal.getCurrentOffset().get()) {
                                            logger.warn("Handled older signal {} for TaskImpl later found: {}", signal, previousSignalFromThisNodeForThisTask);
                                        }
                                    }
                                    lastSignalPerTaskAndNode.get(signal.taskName).put(signal.nodeProcThreadId, signal);
                                    logger.info("SignalHandler handle signal {} from {} for TaskImpl {}/{}", signal.signal,
                                            signal.nodeProcThreadId, task.getDefinition(), task.getLocalState());
                                    node.getSignalHandler().handleSignal(task, signal);
                                }
                                else {
                                    logger.info("Received Signal from unknown task {}", signal);
                                    unmatchedSignals.add(signal);
                                }
                            }
                            else if(r.value().startsWith("{\"name\":")) {
                                NodeTaskInformationImpl nodeInformation = JsonMarshaller.gson.fromJson(r.value(), NodeTaskInformationImpl.class);
                                nodeInformation.setOffset(r.offset());
                                nodeInformation.setArrivalTime(node.getNow());
                                lastNodeInformation.put(nodeInformation.getName(), nodeInformation);
                                node.getNodeTaskInformationHandler().handle(nodeInformation);
                            }
                            else {
                                throw new KctmException("Unexpected event of type: " + r.value() + " on synctopic");
                            }
                        }
                    });
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        if(!doShutdown()) {
                            logger.error("SignalsWatcher N: got interrupted {}", node.getUniqueNodeId(), e);
                        }
                        else {
                            logger.info("SignalsWatcher N: got interrupted {}", node.getUniqueNodeId(), e);
                        }
                        return;
                    }
                }
            }
        }
    }

    static class ConsumerData {
        public ConsumerData(final long startOffset, final long endOffset, final TopicPartition partition) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.partition = partition;
        }

        public ConsumerData(final TopicPartition partition) {
            this.startOffset = -1L;
            this.endOffset = -1L;
            this.partition = partition;
        }

        boolean noOffsets() {
            return startOffset < 0 || endOffset < 0;
        }


        long startOffset;
        long endOffset;
        TopicPartition partition;
    }

    static ConsumerData initConsumer(String topic, final KafkaConsumer<String, String> consumer) {
        List<PartitionInfo> partitionInfo = consumer.listTopics().get(topic);
        if(partitionInfo != null && partitionInfo.size() != 1) {
            throw new KctmException("Either topic " + topic + " not found or more than one partition defined");
        }
        PartitionInfo thePartitionInfo = partitionInfo.get(0);
        TopicPartition partition = new TopicPartition(topic, thePartitionInfo.partition());
        consumer.assign(Collections.singletonList(partition));
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singletonList(partition), Duration.ofSeconds(2));
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(partition), Duration.ofSeconds(2));
        if(endOffsets.size() != 1 && beginningOffsets.size() != 1) {
            return new ConsumerData(partition);
        }
        long endOffset = endOffsets.get(partition).longValue();
        long beginningOffset = beginningOffsets.get(partition).longValue();
        return new ConsumerData(beginningOffset, endOffset, partition);
    }

    static Map<String, Object> getSyncingConsumerConfig(String bootstrapServers, String groupId) {
        Map<String, Object> syncingConsumerConfig = new HashMap<>();
        syncingConsumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        syncingConsumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        syncingConsumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        syncingConsumerConfig.put(MAX_POLL_RECORDS_CONFIG, 100);
        syncingConsumerConfig.put(MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        syncingConsumerConfig.put(GROUP_ID_CONFIG, groupId);
        syncingConsumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        return syncingConsumerConfig;
    }

    /**
     * A map of the most recent nodeinformation arrived from other nodes or nullNodeTaskInformation if the node is
     *      * identified as not running anymore.
     * @return A map of the most recent nodeinformation arrived from other nodes
     */
    public Map<String, NodeTaskInformation> getLastNodeInformation() {
        return lastNodeInformation;
    }

    /**
     * signals older than the first signal consumed by the SignalWatcher
     * @return relevant signals older than the first signal consumed by the SignalWatcher
     */
    public ArrayList<Signal> getOldSignals() {
        return oldSignals;
    }

    /**
     * Signals arrived after SignalsWatcher started which could not get matched against registered tasks yet.
     * @return Signals arrived after SignalsWatcher started which could not get matched against registered tasks yet.
     */
    public ArrayList<Signal> getUnmatchedSignals() {
        return unmatchedSignals;
    }
}
