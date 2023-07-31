package net.oneandone.kafka.clusteredjobs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    protected int outputSignals() {
        Instant startReading = Instant.now();
        ArrayList<ConsumerRecord<String, String>> records = new ArrayList<>();
        SignalsWatcher.iterateOldRecords(
                TestResources.SYNC_TOPIC,
                testResources.getCluster().bootstrapServers(),
                "TestBase",
                r -> {
                    records.add(r);
                });

        Instant endReading = Instant.now();
        records.sort((r1, r2) -> ((Long)r1.offset()).compareTo(r2.offset()));
        logger.info("outputSignals needed {} millis for reading, {} millis for sorting", Duration.between(startReading, endReading).toMillis(),
                Duration.between(endReading, Instant.now()).toMillis());
        records.forEach(r -> {
            if(r.value().contains("ignal")) {
                Signal s = JsonMarshaller.gson.fromJson(r.value(), Signal.class);
                logger.info(String.format("TestUsecases: O: %4d TS: %10s, N: %20s TaskImpl: %10s Signal: %10s Time: %s Ref: %d",
                        r.offset(),Instant.ofEpochMilli(r.timestamp()).toString(),
                        s.getNodeProcThreadId(),
                        s.getTaskName(),
                        s.getSignal(), s.getTimestamp(), s.getReference()));
            } else {
                logger.info(String.format("TestUsecases: O: %4d TS: %10s, J: %s",r.offset(),Instant.ofEpochMilli(r.timestamp()).toString(),  r.value()));
            }
        });
        return records.size();
    }


}
