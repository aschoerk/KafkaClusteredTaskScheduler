package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
public class Sender {
    static Logger logger = LoggerFactory.getLogger(Sender.class);

    private final Node node;
    private final String syncTopic;
    private final KafkaProducer syncProducer;

    public Sender(String syncTopic, Node node) {
        this.node = node;
        this.syncTopic = syncTopic;
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.bootstrapServers);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        this.syncProducer = new KafkaProducer(config);
    }
    void sendAsynchronous(Task t, TaskSignalEnum signal) {
        new Thread(() -> {
            sendSynchronous(t, signal);
        }).start();
    }

    void sendSynchronous(final Task t, final TaskSignalEnum signal) {
        logger.info("Sending from Node: {} for task {} Signal: {}", node.getUniqueNodeId(), t.getName(), signal);
        NodesTaskSignal toSend = new NodesTaskSignal();
        toSend.taskName = t.getName();
        toSend.nodeProcThreadId = node.getUniqueNodeId();
        toSend.signal = signal;
        toSend.timestamp = Instant.now();
        toSend.currentOffset = -1;
        syncProducer.send(new ProducerRecord(syncTopic, node.getUniqueNodeId(), KbXStream.jsonXStream.toXML(toSend)));
        syncProducer.flush();
    }

}
