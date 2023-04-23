package net.oneandone.kafka.clusteredjobs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import jakarta.inject.Inject;

/**
 * @author aschoerk
 */
public abstract class TestBase {

    @Inject
    private TestResources testResources;

    @BeforeEach
    void beforeEachTestBase() throws Exception {
        testResources.startKafka();
    }

    @AfterEach
    void afterEachTestBase() {
        testResources.stopKafkaCluster();
    }

}
