package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.support.TestTask.TestTaskBuilder.aTestTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.oneandone.iocunit.IocJUnit5Extension;
import com.oneandone.iocunit.analyzer.annotations.TestClasses;

import jakarta.inject.Inject;
import net.oneandone.kafka.clusteredjobs.support.TestTask;

@ExtendWith(IocJUnit5Extension.class)
@TestClasses({TestResources.class})
public class TestUsecases extends TestBase {

    public static final int nodeTaskCount = 10;
    @Inject
    TestResources testResources;
    private final int cycleNum = 20;
    private final int waitMillis = 10000;

    boolean runningDuringRelease() {
        // return System.getProperty("surefire.test.class.path") != null;
        return System.getProperty("doingRelease") != null;
    }

    void cycle(String testName, TestTask testTask) throws InterruptedException {
        cycle(testName, testTask, cycleNum, waitMillis, () -> {
            outputSignals();
        });
    }

    void cycle(String testName, TestTask testTask, Runnable r) throws InterruptedException {
        cycle(testName, testTask, cycleNum, waitMillis, r);
    }

    void cycle(String testName, TestTask testTask, int maxCycles, int millisToLetRun) throws InterruptedException {
        cycle(testName, testTask, maxCycles, millisToLetRun, null);
    }
    void cycle(String testName, TestTask testTask, int maxCycles, long millisToLetRun, Runnable r) throws InterruptedException {
        int count = 0;
        int signalNumber = 0;
        while (count < maxCycles) {
            count++;
            long currentExecutions = testTask.getExecutions();
            Thread.sleep(millisToLetRun);
            logger.info("Test {} Loop {} Done: Executions of {}: {} Diff: {}",testName, count   , testTask.getName(),
                    testTask.getExecutions(), testTask.getExecutions() - currentExecutions);
            if (r != null) {
                r.run();
            }
            int num = outputSignals();
            assertNotEquals(signalNumber, num);
            signalNumber = num;
        }
        count++;

    }

    @Test
    void testUsingTwoNodesWithoutTasks() throws InterruptedException {
        if (runningDuringRelease()) {
            return;
        }
        NodeImpl node1 = newNode();
        NodeImpl node2 = newNode();
        final TestTask taskDefinition = aTestTask().withName("notRegistered").build();
        cycle("testUsingOneNode", taskDefinition, 5, node1.getHeartBeatPeriod().multipliedBy(5).toMillis(), () -> {
            assertEquals(0, taskDefinition.getInterrupted().get());
        });
        node1.shutdown();
        node2.shutdown();
        checkLogs();
    }

    @Test
    void testUsingOneNode() throws InterruptedException {
        if (runningDuringRelease()) {
            return;
        }
        NodeImpl node1 = newNode();
        final TestTask taskDefinition = aTestTask().withName("TestTask").withMaxExecutionsOnNode(5L).withPeriod(Duration.ofMillis(500)).build();
        node1.register(taskDefinition);
        cycle("testUsingOneNode", taskDefinition, 5, 5000, () -> {
            assertEquals(0, taskDefinition.getInterrupted().get());
        });
        node1.shutdown();
        checkLogs();
    }

    @Test
    void testMaxExecutionTime() throws InterruptedException {
        if (runningDuringRelease()) {
            return;
        }
        final TestTask testTask = aTestTask()
                .withName("TestTask")
                .withHandlingDuration(Duration.ofMillis(500))
                .withPeriod(Duration.ofMillis(1000))
                .withMaxDuration(Duration.ofMillis(100))
                .withMaxExecutionsOnNode(5L)
                .build();
        NodeImpl node1 = newNode();
        TaskImpl task = node1.register(testTask);
        cycle("tstMaxExecutionTime", testTask, 5, 5000, () -> {
            assertTrue(testTask.getInterrupted().get() > 0);
        });
        node1.shutdown();
        checkLogs();
    }

    @Test
    void testUsingTwoNodes() throws InterruptedException {
        if (runningDuringRelease()) {
            return;
        }
        final TestTask testTask = aTestTask()
                .withName("TestTask")
                .withHandlingDuration(Duration.ofMillis(5))
                .withPeriod(Duration.ofMillis(10))
                .withMaxExecutionsOnNode(5L)
                .build();
        NodeImpl node1 = newNode();
        node1.register(testTask);
        NodeImpl node2 = newNode();
        node2.register(testTask);
        cycle("testUsingTwoNodes",testTask, 5, 10000);
        node1.shutdown();
        node2.shutdown();
        checkLogs();
    }

    @Test
    void testUsingTwoNodesShuttingDown() throws InterruptedException {
        if (runningDuringRelease()) {
            return;
        }
        NodeImpl node1 = newNode();
        final TestTask heartBeat1 = aTestTask().withMaxExecutionsOnNode(5L).withName("Test1_1").build();
        final TestTask heartBeat2 = aTestTask().withName("Test1_2").withHandlingDuration(Duration.ofMillis(nodeTaskCount)).build();
        node1.register(heartBeat1);
        node1.register(heartBeat2);
        final MutableObject<NodeImpl> node2 = new MutableObject<>(newNode());
        node2.getValue().register(heartBeat1);
        node2.getValue().register(heartBeat2);
        node2.getValue().run();
        cycle("testUsingTwoNodesShuttingDown", heartBeat1,  5, 10000, () -> {
            node2.getValue().shutdown();
            node2.setValue(newNode());
            node2.getValue().register(heartBeat1);
            node2.getValue().register(heartBeat2);
            outputSignals();
        });
        node1.shutdown();
        node2.getValue().shutdown();
        checkLogs();
    }

    @Test
    void testUsingTenNodesAnd10Tasks() throws InterruptedException {
        if (runningDuringRelease()) {
            return;
        }
        Random random = new Random();
        ArrayList<NodeImpl> nodes = new ArrayList<>();
        List<TestTask> definitions = new ArrayList<>();
        for (int i = 0; i < nodeTaskCount; i++) {
            nodes.add(newNode());
            definitions.add(aTestTask().withName("TestTask" + i).withPeriod(Duration.ofMillis(100L + (100L * i))).build());
        }
        final TestTask taskDefinition = aTestTask().withName("Testtask1").build();
        definitions.forEach(d -> {
            nodes.forEach(n -> n.register(d));
        });
        cycle("testUsingTenNodesAnd10Tasks",definitions.get(0), () -> {
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
        });
        logger.warn("Completed test");
        checkLogs();
    }

    private static void checkLogs() {
        Assertions.assertEquals(0, InterceptingAppender.countErrors.get(), "errors");
        Assertions.assertEquals(0, InterceptingAppender.countWarnings.get(), "warnings");
        assertTrue(InterceptingAppender.countElse.get() > 0);
    }

    @Test
    void test() throws InterruptedException {
        if (runningDuringRelease()) {
            return;
        }
        NodeImpl node1 = newNode();
        final TestTask taskDefinition = aTestTask().withName("Testtask1").build();
        node1.register(taskDefinition);
        NodeImpl node2 = newNode();
        node2.register(taskDefinition);

        node1.run();
        node2.run();

        int count = 0;
        while (count < 20) {
            count++;
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
            outputSignals();
        }
        count++;
        checkLogs();
    }
}
