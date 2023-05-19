package net.oneandone.kafka.clusteredjobs;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one component used by the node components to send signals via sync-topic
 */
public class Sender {
    static Logger logger = LoggerFactory.getLogger(Sender.class);

    private final NodeImpl node;
    private final String syncTopic;
    private KafkaProducer syncProducer;
    private Map<String, Object> config;

    /**
     * create the SignalSender for this node
     * @param node the node the sender to create for.
     */
    public Sender(NodeImpl node) {
        this.node = node;
        this.syncTopic = node.syncTopic;
        config = getConfig(node);
    }

    static Map<String, Object> getConfig(final NodeImpl node) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node.bootstrapServers);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return config;
    }

    void sendSignal(final Task t, final SignalEnum signal) {
        logger.info("Sending from N: {} for task {} int State: {} Signal: {}",
                                        node.getUniqueNodeId(),
                t != null ? t.getDefinition().getName() : "NodeTask", t != null ? t.getLocalState() : "null", signal);
        Signal toSend = new Signal();
        if (t != null) {
            toSend.taskName = t.getDefinition().getName();
        } else {
            toSend.taskName = "NodeTask";
        }
        toSend.nodeProcThreadId = node.getUniqueNodeId();
        toSend.signal = signal;
        toSend.timestamp = node.getNow();
        getSyncProducer().send(new ProducerRecord(syncTopic, node.getUniqueNodeId(), KbXStream.jsonXStream.toXML(toSend)));
        syncProducer.flush();
    }

    KafkaProducer getSyncProducer() {
        if (syncProducer == null) {
            this.syncProducer = new KafkaProducer(config);
        }
        return syncProducer;
    }
}
