package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;

import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oneandone.iocunit.IocJUnit5Extension;
import com.oneandone.iocunit.analyzer.annotations.TestClasses;

import jakarta.inject.Inject;

@ExtendWith(IocJUnit5Extension.class)
@TestClasses({TestResources.class})
public class Test extends TestBase{


     static Logger logger = LoggerFactory.getLogger(Test.class);

     @Inject
     TestResources testResources;


    @org.junit.jupiter.api.Test
    void test() throws InterruptedException {
        Node node1 = new Node(TestResources.SYNC_TOPIC, testResources.getCluster().bootstrapServers());
        node1.register(new HeartBeatTask(node1.getUniqueNodeId()));
        Node node2 = new Node(TestResources.SYNC_TOPIC, testResources.getCluster().bootstrapServers());
        node2.register(new HeartBeatTask(node2.getUniqueNodeId()));

        node1.run();
        node2.run();

        while (true) {
            Thread.sleep(10000);
            logger.info("TestLoop {}", Instant.now());
            node1.shutdown();
            Thread.sleep(1000);
            node1 = new Node(TestResources.SYNC_TOPIC, testResources.getCluster().bootstrapServers());
            node1.register(new HeartBeatTask(node1.getUniqueNodeId()));
            node1.run();
            node2.shutdown();
            Thread.sleep(1000);
            node2 = new Node(TestResources.SYNC_TOPIC, testResources.getCluster().bootstrapServers());
            node2.register(new HeartBeatTask(node2.getUniqueNodeId()));
            node2.run();
        }
    }
}
