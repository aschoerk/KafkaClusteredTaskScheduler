package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.api.StateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.support.TestTask.TestTaskBuilder.aTestTask;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import net.oneandone.kafka.clusteredjobs.api.StateEnum;
import net.oneandone.kafka.clusteredjobs.support.TestTask;
import net.oneandone.kafka.clusteredjobs.support.TestContainer;

public class SignalHandlerTest {

    static class TestNodeFactory extends NodeFactoryImpl {
        SignalEnum signalSent = null;
        @Override
        public Sender createSender(final NodeImpl node) {
            return  new Sender(node) {
                @Override
                public void sendSignal(final TaskImpl t, final SignalEnum signal) {
                    signalSent = signal;
                }
            };
        }

        @Override
            public NodeTaskInformationHandler createNodeTaskInformationHandler(final NodeImpl node) {
            return new NodeTaskInformationHandler(node) {
                @Override
                public Optional<Pair<String, SignalEnum>> getUnknownTaskSignal(final String taskname) {
                    return Optional.empty();
                }
            };
        }

        @Override
        public SignalsWatcher createSignalsWatcher(final NodeImpl node) {
            return new SignalsWatcher(node)  {
                @Override
                public void run() {
                    setRunning();
                    synchronized (node) {

                        node.notify();
                    }
                }

                @Override
                void readOldSignals() {
                    ;
                }

                @Override
                public ConsumerData getWatcherStarting() {
                    return new ConsumerData(0, 0, null);
                }
            };
        }
    }


    @ParameterizedTest
    @CsvSource({
            "INITIATING_I,ME,NEW,INITIATING,",
            "INITIATING_I,ME,CLAIMING,ERROR,",
            "INITIATING_I,ME,CLAIMED_BY_OTHER,ERROR,",
            "INITIATING_I,ME,HANDLING_BY_OTHER,ERROR,",
//            "DOHEARTBEAT,a,CLAIMED_BY_NODE,CLAIMED_BY_NODE,HEARTBEAT",
//            "DOHEARTBEAT,a,HANDLING_BY_NODE,HANDLING_BY_NODE,HANDLING",
//            "DOHEARTBEAT,a,NEW,NEW,",
//            "DOHEARTBEAT,ME,NEW,NEW,",
//            "DOHEARTBEAT,ME,INITIATING,ERROR,",  // I did wrong
//            "DOHEARTBEAT,ME,CLAIMING,ERROR,", // I was first
//            "DOHEARTBEAT,ME,CLAIMED_BY_OTHER,ERROR,",
//            "DOHEARTBEAT,ME,HANDLING_BY_OTHER,ERROR,",
//            "DOHEARTBEAT,ME,CLAIMED_BY_NODE,ERROR,",
//            "DOHEARTBEAT,ME,HANDLING_BY_NODE,ERROR,",
            "CLAIMING,a,NEW,CLAIMED_BY_OTHER,",  // another one was first
            "CLAIMING,a,CLAIMING,CLAIMED_BY_OTHER,", // another one was first
            "CLAIMING,a,CLAIMED_BY_OTHER,CLAIMED_BY_OTHER,",  // loser in competition
            "CLAIMING,a,HANDLING_BY_OTHER,HANDLING_BY_OTHER,",  // loser in competition
            "CLAIMING,a,CLAIMED_BY_NODE,CLAIMED_BY_NODE,CLAIMED",  // loser in competition
            "CLAIMING,a,HANDLING_BY_NODE,HANDLING_BY_NODE,CLAIMED",  // loser in competition
            "CLAIMING,ME,NEW,ERROR,",  // I did wrong
            "CLAIMING,ME,INITIATING,ERROR,",  // I did wrong
            "CLAIMING,ME,CLAIMING,CLAIMED_BY_NODE,", // I was first
            "CLAIMING,ME,CLAIMED_BY_OTHER,CLAIMED_BY_OTHER,",
            "CLAIMING,ME,HANDLING_BY_OTHER,HANDLING_BY_OTHER,",
            "CLAIMING,ME,CLAIMED_BY_NODE,ERROR,",
            "CLAIMING,ME,HANDLING_BY_NODE,ERROR,",
            "CLAIMED,a,NEW,CLAIMED_BY_OTHER,",  //
            "CLAIMED,a,INITIATING,ERROR,",  // CLAIMING should have been seen first
            "CLAIMED,a,CLAIMING,ERROR,", // CLAIMING should have arrived earlier
            "CLAIMED,a,CLAIMED_BY_OTHER,CLAIMED_BY_OTHER,",  // because of preventing answer from other node
            "CLAIMED,a,HANDLING_BY_OTHER,CLAIMED_BY_OTHER,",  // there should have been a HANDLING
            "CLAIMED,a,CLAIMED_BY_NODE,ERROR,",  // error in protocol
            "CLAIMED,a,HANDLING_BY_NODE,ERROR,",  // there should have been a UNCLAIMED, CLAIMING
            "CLAIMED,ME,NEW,ERROR,",  // must not send signals while new
            "CLAIMED,ME,INITIATING,ERROR,",  // first CLAIMING is necessary
            "CLAIMED,ME,CLAIMING,ERROR,", // I was first, so state must be CLAIMED_BY_NODE
            "CLAIMED,ME,CLAIMED_BY_OTHER,ERROR,",  //
            "CLAIMED,ME,HANDLING_BY_OTHER,ERROR,",  //
            "CLAIMED,ME,CLAIMED_BY_NODE,CLAIMED_BY_NODE,",  // ok
            "CLAIMED,ME,HANDLING_BY_NODE,HANDLING_BY_NODE,",  // CLAIMED_BY_NODE goes without signal evaluation to HANDLING_BY_NODE
            "HEARTBEAT,a,NEW,CLAIMED_BY_OTHER,",  // heartbeat during task-init, possible
            "HEARTBEAT,a,INITIATING,CLAIMED_BY_OTHER,",  // old heartbeat during task-init, possible
            "HEARTBEAT,a,CLAIMING,ERROR,", // CLAIMED should have arrived first
            "HEARTBEAT,a,CLAIMED_BY_OTHER,CLAIMED_BY_OTHER,",  // regular information
            "HEARTBEAT,a,HANDLING_BY_OTHER,CLAIMED_BY_OTHER,",  // CLAIMED should have been sent earlier
            "HEARTBEAT,a,CLAIMED_BY_NODE,ERROR,",  // error in protocol
            "HEARTBEAT,a,HANDLING_BY_NODE,ERROR,",  // error in protocol
            "HEARTBEAT,ME,NEW,ERROR,",  // must not send anything in new state
            "HEARTBEAT,ME,INITIATING,ERROR,",  // old heartbeat ??
            "HEARTBEAT,ME,CLAIMING,ERROR,", // old heartbeat??
            "HEARTBEAT,ME,CLAIMED_BY_OTHER,ERROR,",  // error I must have received CLAIMING, CLAIMED meanwhile
            "HEARTBEAT,ME,HANDLING_BY_OTHER,ERROR,",  // error I must have received CLAIMING, CLAIMED meanwhile
            "HEARTBEAT,ME,CLAIMED_BY_NODE,CLAIMED_BY_NODE,",  // ok
            "HEARTBEAT,ME,HANDLING_BY_NODE,HANDLING_BY_NODE,",  // could be an old heartbeat
            "HANDLING,a,NEW,HANDLING_BY_OTHER,",  //
            "HANDLING,a,INITIATING,ERROR,",  // CLAIMED should have arrived earlier
            "HANDLING,a,CLAIMING,ERROR,", // CLAIMED should have arrived earlier
            "HANDLING,a,CLAIMED_BY_OTHER,HANDLING_BY_OTHER,",  // ok
            "HANDLING,a,HANDLING_BY_OTHER,HANDLING_BY_OTHER,",  // allow because of NEW-state
            "HANDLING,a,CLAIMED_BY_NODE,ERROR,",  // error in protocol
            "HANDLING,a,HANDLING_BY_NODE,ERROR,",  // error in protocol
            "HANDLING,ME,NEW,ERROR,",  // must not send anything in new state
            "HANDLING,ME,INITIATING,ERROR,",  // I did wrong
            "HANDLING,ME,CLAIMING,ERROR,", // I was first, so state should be HANDLING_BY_NODE
            "HANDLING,ME,CLAIMED_BY_OTHER,ERROR,",  //
            "HANDLING,ME,HANDLING_BY_OTHER,ERROR,",  //
            "HANDLING,ME,CLAIMED_BY_NODE,CLAIMED_BY_NODE,",  // old handling message
            "HANDLING,ME,HANDLING_BY_NODE,HANDLING_BY_NODE,",  // old handling message
            "UNCLAIMED,a,NEW,INITIATING,",  // opportunity
            "UNCLAIMED,a,INITIATING,INITIATING,",  // TODO:
            "UNCLAIMED,a,CLAIMING,CLAIMING,", // TODO:
            "UNCLAIMED,a,CLAIMED_BY_OTHER,INITIATING,",  // ok
            "UNCLAIMED,a,HANDLING_BY_OTHER,INITIATING,",  // TODO: alive messages during handling-interval?? No!! use resurrection
            "UNCLAIMED,a,CLAIMED_BY_NODE,ERROR,",  // error in protocol, other node unclaims my task
            "UNCLAIMED,a,HANDLING_BY_NODE,ERROR,",  // error in protocol, other node unclaims my task
            "UNCLAIMED,ME,NEW,ERROR,",  // must not send anything in new state
            "UNCLAIMED,ME,INITIATING,INITIATING,",  // old unclaimed message
            "UNCLAIMED,ME,CLAIMING,CLAIMING,", // old unclaimed message, read my own message
            "UNCLAIMED,ME,CLAIMED_BY_OTHER,ERROR,",  // error in protocol CLAIMED by me should have arrived earlier
            "UNCLAIMED,ME,HANDLING_BY_OTHER,ERROR,",  // error in protocol CLAIMED,HANDLING by me should have arrived earlier
            "UNCLAIMED,ME,CLAIMED_BY_NODE,ERROR,",  // error in protocol CLAIMED by me should have arrived earlier
            "UNCLAIMED,ME,HANDLING_BY_NODE,ERROR,",  // error in protocol CLAIMED,HANDLING by me should have arrived earlier
    })
    void testStateEngine(SignalEnum signal, String senderNode, StateEnum localState, StateEnum newState, SignalEnum expectedSignal) {
        final TestContainer container = new TestContainer(TestResources.SYNC_TOPIC, "dummyNodes");
        final TestNodeFactory nodeFactory = new TestNodeFactory();
        NodeImpl node = new NodeImpl(container, nodeFactory);
        node.run();
        if(senderNode.equals("ME")) {
            senderNode = node.getUniqueNodeId();
        }

        TestTask testTask = aTestTask().build();
        TaskImpl task = node.register(testTask);
        task.localState = localState;
        Signal signalReceived = new Signal();
        signalReceived.taskName = testTask.getName();
        signalReceived.nodeProcThreadId = senderNode;
        signalReceived.signal = signal;
        signalReceived.setCurrentOffset(0L);
        node.getSignalHandler().handleSignal(task, signalReceived);

        Assertions.assertEquals(newState, task.getLocalState());
        if(expectedSignal != null) {
            Assertions.assertEquals(expectedSignal, nodeFactory.signalSent);
        }

    }

    @Test
    void testInitiatingI() {
        // TODO: test startup/register of task in different situations
    }

    @Test
    void testUnclaimI() {
        // TODO: unclaimI may be called in different situations
    }
}
