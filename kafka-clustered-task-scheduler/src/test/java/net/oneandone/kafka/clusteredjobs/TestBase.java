package net.oneandone.kafka.clusteredjobs;

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

    Logger logger = LoggerFactory.getLogger(TestBase.class);

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
        NodeImpl result = new NodeImpl(new TestContainer(TestResources.SYNC_TOPIC, testResources.getCluster().bootstrapServers()));
        result.run();
        return result;
    }


}
