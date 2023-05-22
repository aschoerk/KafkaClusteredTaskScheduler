package net.oneandone.kafka.clusteredjobs;

import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import net.oneandone.kafka.clusteredjobs.support.TestContainer;

/**
 * @author aschoerk
 */
public abstract class TestBase {

    static Logger logger = LoggerFactory.getLogger("TestBase");

    @Inject
    private TestResources testResources;

    @BeforeEach
    void beforeEachTestBase() throws Exception {
        logger.info("Starting Kafka");
        testResources.startKafka();
    }

    @AfterEach
    void afterEachTestBase() {
        testResources.stopKafkaCluster();
    }

    protected NodeImpl newNode() {
        NodeImpl result = new NodeImpl(new TestContainer(TestResources.SYNC_TOPIC, testResources.getCluster().bootstrapServers()), new NodeFactoryImpl());
        result.run();
        return result;
    }

    protected void outputSignals() {
        ArrayList<ConsumerRecord<String, String>> records = new ArrayList<>();
        SignalsWatcher.iterateOldRecords(
                TestResources.SYNC_TOPIC,
                testResources.getCluster().bootstrapServers(),
                "TestBase",
                r -> {
                    records.add(r);
                });

        records.sort((r1, r2) -> ((Long)r1.offset()).compareTo(r2.offset()));
        records.forEach(r -> {
            Object event = KbXStream.jsonXStream.fromXML(r.value());
            if(event instanceof Signal) {
                Signal s = (Signal)event;
                logger.info(String.format("Test1: O: %4d N: %20s Task: %10s Signal: %10s Time: %s",r.offset(),  s.nodeProcThreadId, s.taskName, s.signal, s.timestamp));
            } else {
                logger.info(String.format("Test1: O: %4d J: %s",r.offset(),  r.value()));

            }
        });
    }


}
