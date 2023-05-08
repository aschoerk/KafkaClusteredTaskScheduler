package net.oneandone.kafka.clusteredjobs;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import kafka.server.KafkaConfig;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oneandone.iocunit.analyzer.annotations.SutClasses;


@ApplicationScoped
public class TestResources {

    @Produces
    public Clock produceClock() {
        return Clock.systemUTC();
    }
    public static final String SYNC_TOPIC = "kafka-sync-topic";
    static final Logger LOGGER = LoggerFactory.getLogger(TestResources.class);
    private TestCluster cluster;

    public void startKafka() throws Exception {
        Properties brokerConfig = new Properties();
        brokerConfig.setProperty(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:0");
        cluster = new TestCluster(1, brokerConfig);
        cluster.start();

        cluster.deleteTopicAndWait(SYNC_TOPIC);
        cluster.createTopic(SYNC_TOPIC, 1, 1);

    }



    public void stopKafkaCluster() {
        if (cluster != null)
            cluster.shutdown();
    }

    public TestCluster getCluster() {
        return cluster;
    }

    public static class TestCluster extends EmbeddedKafkaCluster {
        public TestCluster(int numBrokers, Properties config) {
            super(numBrokers, config);
        }

        public void shutdown() {
            stop();
        }
    }


}
