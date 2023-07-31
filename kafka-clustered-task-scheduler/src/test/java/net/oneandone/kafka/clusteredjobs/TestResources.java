package net.oneandone.kafka.clusteredjobs;

import java.time.Clock;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import kafka.server.KafkaConfig;


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
        if (cluster != null) {
            cluster.shutdown();
        }
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
