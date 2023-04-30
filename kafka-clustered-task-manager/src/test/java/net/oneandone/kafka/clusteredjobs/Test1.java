package net.oneandone.kafka.clusteredjobs;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oneandone.iocunit.IocJUnit5Extension;
import com.oneandone.iocunit.analyzer.annotations.TestClasses;

import jakarta.inject.Inject;

@ExtendWith(IocJUnit5Extension.class)
@TestClasses({TestResources.class})
public class Test1 extends TestBase{


     static Logger logger = LoggerFactory.getLogger(Test1.class);

     @Inject
     TestResources testResources;

     @ParameterizedTest
     @CsvSource({
             "200,200,140",
             "200,240,180",
             "10,240,0",
             "11,240,10",
             "200,100,40",
     })
     void canPositionBeforeWatchingSignals(int seconds, int positionInSeconds,int result) {
         Node node = new Node(TestResources.SYNC_TOPIC, testResources.getCluster().bootstrapServers());
         Task task = new HeartBeatTask(node.getUniqueNodeId());
         node.register(task);
         final Clock baseClock = Clock.fixed(Instant.now().minus(200, ChronoUnit.DAYS), ZoneId.of("CET"));
         node.setClock(baseClock);

         Sender sender = new Sender(node);
         for(int i = 0; i < seconds; i++ ) {
             node.setClock(Clock.offset(baseClock, Duration.ofSeconds(i)));
             sender.sendSynchronous(task, TaskSignalEnum.CLAIMING);
         }


         SignalsWatcher signalsWatcher = new SignalsWatcher(node, sender);
         node.setClock(Clock.offset(baseClock, Duration.ofSeconds(positionInSeconds)));
         Pair<TopicPartition, Long> position = signalsWatcher.findConsumerPosition(SignalsWatcher.getSyncingConsumerConfig(node), 10);
         Assertions.assertEquals(result, position.getRight());
     }

    @Test
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
