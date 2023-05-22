package net.oneandone.kafka.clusteredjobs.states;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.HANDLING;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.HANDLING_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNCLAIM_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.UNHANDLING_I;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.MutableObject;

import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.Signal;
import net.oneandone.kafka.clusteredjobs.SignalEnum;
import net.oneandone.kafka.clusteredjobs.Task;

/**
 * @author aschoerk
 */
public class ClaimedByNode extends StateHandlerBase {
    public ClaimedByNode(NodeImpl node) {
        super(node, StateEnum.CLAIMED_BY_NODE);
    }

    long handlerThreadCounter = 0L;



    @Override
    protected void handleSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case CLAIMING:
                getNode().getSender().sendSignal(task, SignalEnum.CLAIMED);
                break;
            default:
                super.handleSignal(task, s);
        }
    }

    @Override
    protected void handleOwnSignal(final Task task, final Signal s) {
        switch (s.getSignal()) {
            case HEARTBEAT:
            case CLAIMED:
            case HANDLING:
                info(task,s,"ignored");
                break;
            default:
                super.handleOwnSignal(task, s);
        }
    }

    @Override
    protected void handleInternal(final Task task, final Signal s) {
        if (s.getSignal().equals(UNCLAIM_I)) {
            startUnclaiming(task);
        } else if (s.getSignal() == HANDLING_I) {
            task.setLocalState(StateEnum.HANDLING_BY_NODE);
            getNode().getSender().sendSignal(task, HANDLING);
            final String threadName = task.getDefinition().getName() + "_" + Thread.currentThread().getId() + "_" + handlerThreadCounter++;
            MutableObject<Thread> p = new MutableObject();
            p.setValue(getNode().newHandlerThread(new Runnable() {
                @Override
                public void run() {
                    try {
                        getNode().getPendingHandler().removeClaimedHeartbeat(task); // claim could get lost when running job
                        task.getDefinition().getCode(getNode()).run();
                    } finally {
                        if(task.getLocalState() == StateEnum.HANDLING_BY_NODE) {
                            getNode().getSignalHandler().handleInternalSignal(task, UNHANDLING_I);
                        }
                        getNode().disposeHandlerThread(p.getValue());
                    }
                }
            }));
            p.getValue().setName(threadName);
            p.getValue().start();
            getNode().getPendingHandler().scheduleInterupter(task, threadName, p.getValue());
        } else {
            super.handleInternal(task, s);
        }
    }
}
