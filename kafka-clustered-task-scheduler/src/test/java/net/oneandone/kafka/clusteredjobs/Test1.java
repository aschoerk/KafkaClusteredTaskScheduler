package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.support.HeartBeatTask.HeartBeatTaskBuilder.aHeartBeatTask;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;
import net.oneandone.kafka.clusteredjobs.support.HeartBeatTask;
import net.oneandone.kafka.clusteredjobs.support.TestContainer;

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
         NodeImpl node = newNode();
         TaskDefinition taskDefinition = aHeartBeatTask().build();
         Task task = node.register(taskDefinition);
         final Clock baseClock = Clock.fixed(Instant.now().minus(200, ChronoUnit.DAYS), ZoneId.of("CET"));
         node.setClock(baseClock);

         for(int i = 0; i < seconds; i++ ) {
             node.setClock(Clock.offset(baseClock, Duration.ofSeconds(i)));
             node.getSender().sendSynchronous(task, SignalEnum.CLAIMING);
         }


         SignalsWatcher signalsWatcher = new SignalsWatcher(node);
         node.setClock(Clock.offset(baseClock, Duration.ofSeconds(positionInSeconds)));
         Pair<TopicPartition, Long> position = signalsWatcher.findConsumerPosition(SignalsWatcher.getSyncingConsumerConfig(node), 10);
         Assertions.assertEquals(result, position.getRight());
     }

    @Test
    void testUsingOneNode() throws InterruptedException {
        NodeImpl node1 = newNode();
        final HeartBeatTask taskDefinition = aHeartBeatTask().withName("TestTask").withPeriod(Duration.ofMillis(500)).build();
        node1.register(taskDefinition);
        int count = 0;
        while (count++ < 20) {
            Thread.sleep(10000);
            logger.info("TestLoop {}", Instant.now());
        }
        node1.shutdown();
        checkLogs();
    }

    @Test
    void testUsingTwoNodes() throws InterruptedException {
        final HeartBeatTask testTask = aHeartBeatTask().withName("TestTask").withMaxExecutionsOnNode(5L).build();
        NodeImpl node1 = newNode();
        node1.register(testTask);
        NodeImpl node2 = newNode();
        node2.register(testTask);
        int count = 0;
        while (count++ < 20) {
            Thread.sleep(10000);
            logger.info("TestLoop {}", Instant.now());
        }
        node1.shutdown();
        node2.shutdown();
        checkLogs();
    }

    @Test
    void testUsingTwoNodesShuttingDown() throws InterruptedException {
        NodeImpl node1 = newNode();
        final HeartBeatTask heartBeat1 = aHeartBeatTask().withMaxExecutionsOnNode(5L).withName("Test1_1").build();
        final HeartBeatTask heartBeat2 = aHeartBeatTask().withName("Test1_2").withHeartBeatDuration(Duration.ofMillis(10)).build();
        node1.register(heartBeat1);
        node1.register(heartBeat2);
        NodeImpl node2 = newNode();
        node2.register(heartBeat1);
        node2.register(heartBeat2);
        node2.run();
        int count = 0;
        while (count++ < 20) {
            Thread.sleep(3000);
            logger.info("TestLoop {}", Instant.now());
            node2.shutdown();
            node2 = newNode();
            node2.register(heartBeat1);
            node2.register(heartBeat2);
        }
        node1.shutdown();
        node2.shutdown();
        checkLogs();
    }

    @Test
    void testUsingTenNodesAnd10Tasks() throws InterruptedException {
        Random random = new Random();
        ArrayList<NodeImpl> nodes = new ArrayList<>();
        List<HeartBeatTask> definitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nodes.add(newNode());
            definitions.add(aHeartBeatTask().withName("TestTask" + i).withPeriod(Duration.ofMillis(100L + 100L * i)).build());
        }
        final HeartBeatTask taskDefinition = aHeartBeatTask().withName("Testtask1").build();
        definitions.forEach(d -> {
            nodes.forEach(n -> n.register(d));
        });
        int count = 0;
        while (count < 20) {
            Thread.sleep(10000);
            int index = random.nextInt(10);
            NodeImpl node = nodes.get(random.nextInt(10));
            node.shutdown();
            final NodeImpl nodeNew = newNode();
            nodes.set(index, nodeNew);
            definitions.forEach(d -> nodeNew.register(d));
        }
        checkLogs();
    }

    private static void checkLogs() {
        Assertions.assertEquals(0, InterceptingAppender.countErrors.get());
        Assertions.assertEquals(0, InterceptingAppender.countWarnings.get());
        Assertions.assertTrue(InterceptingAppender.countElse.get() > 0);
    }

    @Test
    void test() throws InterruptedException {
        NodeImpl node1 = newNode();
        final HeartBeatTask taskDefinition = aHeartBeatTask().withName("Testtask1").build();
        node1.register(taskDefinition);
        NodeImpl node2 = newNode();
        node2.register(taskDefinition);

        node1.run();
        node2.run();

        int count = 0;
        while (count++ < 20) {
            Thread.sleep(10000);
            logger.info("TestLoop {}", Instant.now());
            node1.shutdown();
            Thread.sleep(1000);
            node1 = newNode();
            node1.register(taskDefinition);
            node1.run();
            node2.shutdown();
            Thread.sleep(1000);
            node2 = newNode();
            node2.register(taskDefinition);
            node2.run();
        }
        checkLogs();
    }
}
