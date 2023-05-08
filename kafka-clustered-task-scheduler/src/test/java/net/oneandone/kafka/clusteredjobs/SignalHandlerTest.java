package net.oneandone.kafka.clusteredjobs;

import java.util.HashMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import net.oneandone.kafka.clusteredjobs.support.HeartBeatTask;
import net.oneandone.kafka.clusteredjobs.support.TestContainer;

/**
 * @author aschoerk
 */
public class SignalHandlerTest {
    SignalEnum signalSent;

    @ParameterizedTest
    @CsvSource({
            "INITIATING,a,CLAIMING,CLAIMED_BY_NODE,"
    })
    void testStateEngine(TaskStateEnum localState, String senderNode, SignalEnum signal, TaskStateEnum newState, SignalEnum expectedSignal) {
        NodeImpl node = new NodeImpl(new TestContainer(TestResources.SYNC_TOPIC, "dummyNodes"));
        if(senderNode == "ME") {
            senderNode = node.getUniqueNodeId();
        }
        Sender dummySender = new Sender(node) {

            @Override
            void sendSynchronous(final Task t, final SignalEnum signal) {
                signalSent = signal;
            }

            ;

        };
        node.setSender(dummySender);
        HeartBeatTask heartBeatTask = new HeartBeatTask();
        Task task = node.register(heartBeatTask);
        task.setLocalState(localState);

        Signal signalReceived = new Signal();
        signalReceived.taskName = heartBeatTask.getName();
        signalReceived.nodeProcThreadId = senderNode;
        signalReceived.signal = signal;
        HashMap<String, Signal> map = new HashMap<>();
        map.put(signalReceived.nodeProcThreadId, signalReceived);
        node.getSignalHandler().handle(heartBeatTask.getName(), map);

        Assertions.assertEquals(newState, task.getLocalState());
        if(expectedSignal != null) {
            Assertions.assertEquals(expectedSignal, signalSent);
        }

    }
}
