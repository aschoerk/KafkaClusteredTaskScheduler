package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.support.HeartBeatTask.HeartBeatTaskBuilder.aHeartBeatTask;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.oneandone.iocunit.IocJUnit5Extension;
import com.oneandone.iocunit.analyzer.annotations.TestClasses;

import jakarta.inject.Inject;
import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;
import net.oneandone.kafka.clusteredjobs.support.HeartBeatTask;

@ExtendWith(IocJUnit5Extension.class)
@TestClasses({TestResources.class})
public class Test1 extends TestBase {

    public static final int nodeTaskCount = 10;
    @Inject
    TestResources testResources;

//    @ParameterizedTest
//    @CsvSource({
//            "200,200,140",
//            "200,240,180",
//            "10,240,0",
//            "11,240,10",
//            "200,100,40",
//    })
    void canPositionBeforeWatchingSignals(int seconds, int positionInSeconds, int result) {
        NodeImpl node = newNode();
        TaskDefinition taskDefinition = aHeartBeatTask().withName("TestTask").withPeriod(Duration.ofMillis(1000)).build();
        Task task = node.register(taskDefinition);
        final Clock baseClock = Clock.fixed(Instant.now().minus(200, ChronoUnit.DAYS), ZoneId.of("CET"));
        node.setClock(baseClock);
        node.getSignalsWatcher().shutdown();


        for (int i = 0; i < seconds; i++) {
            node.setClock(Clock.offset(baseClock, Duration.ofSeconds(i)));
            node.getSender().sendSignal(task, SignalEnum.HEARTBEAT);
        }


        SignalsWatcher signalsWatcher = new SignalsWatcher(node);
        signalsWatcher.run();
        node.setClock(Clock.offset(baseClock, Duration.ofSeconds(positionInSeconds)));
        signalsWatcher.readOldSignals();
        // Pair<TopicPartition, Long> position = signalsWatcher.findConsumerPosition(SignalsWatcher.getSyncingConsumerConfig(node), nodeTaskCount);
        // Assertions.assertEquals(result, position.getRight());
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
        final HeartBeatTask testTask = aHeartBeatTask().withName("TestTask").withHeartBeatDuration(Duration.ofMillis(5)).withPeriod(Duration.ofMillis(10)).withMaxExecutionsOnNode(5L).build();
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
        final HeartBeatTask heartBeat2 = aHeartBeatTask().withName("Test1_2").withHeartBeatDuration(Duration.ofMillis(nodeTaskCount)).build();
        node1.register(heartBeat1);
        node1.register(heartBeat2);
        NodeImpl node2 = newNode();
        node2.register(heartBeat1);
        node2.register(heartBeat2);
        node2.run();
        int count = 0;
        while (count++ < 20) {
            logger.warn("Starting loop {} of 20", count);
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
        for (int i = 0; i < nodeTaskCount; i++) {
            nodes.add(newNode());
            definitions.add(aHeartBeatTask().withName("TestTask" + i).withPeriod(Duration.ofMillis(100L + 100L * i)).build());
        }
        final HeartBeatTask taskDefinition = aHeartBeatTask().withName("Testtask1").build();
        definitions.forEach(d -> {
            nodes.forEach(n -> n.register(d));
        });
        int count = 0;
        while (count++ < 20) {
            logger.warn("Starting loop {} of 20", count);
            Thread.sleep(10000);
            int restartCount = random.nextInt(nodeTaskCount-2)  + 2;
            Set<Integer> toRestartNodes = new HashSet<>();
            for (int i = 0; i <= restartCount; i++) {
                toRestartNodes.add(random.nextInt(nodeTaskCount));
            }
            logger.warn("Now restarting {} nodes", toRestartNodes.size());
            toRestartNodes.forEach(i -> {
                NodeImpl node = nodes.get(i);
                logger.warn("shutdown of {}", node.getUniqueNodeId());
                node.shutdown();
                node = newNode();
                logger.warn("Restart of {}", node.getUniqueNodeId());
                nodes.set(i, node);
            });
            toRestartNodes.forEach(i -> {
                NodeImpl node = nodes.get(i);
                definitions.forEach(d -> {
                    node.register(d);
                });
            });
        }
        logger.warn("Completed test");
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
            logger.warn("Starting loop {} of 20", count);
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
