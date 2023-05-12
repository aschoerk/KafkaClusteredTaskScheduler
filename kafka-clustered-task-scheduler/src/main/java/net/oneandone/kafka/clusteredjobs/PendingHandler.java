package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.HEARTBEAT_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.CLAIMING_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.HANDLING_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.NEW_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.RESURRECTING;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
public class PendingHandler extends StoppableBase {

    Logger logger = LoggerFactory.getLogger(PendingHandler.class);

    SortedSet<PendingEntry> sortedPending = Collections.synchronizedSortedSet(new TreeSet<>(new PendingEntry.TimestampComparator()));

    Map<String, PendingEntry> pendingByIdentifier = Collections.synchronizedMap(new HashMap<>());

    private final NodeImpl node;

    public PendingHandler(final NodeImpl node) {
        this.node = node;
    }


    public void schedulePending(final PendingEntry e) {
        logger.info("Node: {} Scheduling PendingEntry: {}", node.getUniqueNodeId(), e);
        if(pendingByIdentifier.containsKey(e.getIdentifier())) {
            PendingEntry existingTask = pendingByIdentifier.get(e.getIdentifier());
            sortedPending.remove(existingTask);
        }
        pendingByIdentifier.put(e.getIdentifier(), e);
        sortedPending.add(e);
        synchronized (this) {
            if(sortedPending.first().equals(e)) {
                this.notify();
            }
        }
    }


    private void removePending(final String pendingName, boolean enforce) {
        logger.info("Removing pending {}", pendingName);
        PendingEntry e = pendingByIdentifier.get(pendingName);
        pendingByIdentifier.remove(pendingName);
        if(e != null) {
            boolean result = sortedPending.remove(e);
            if (!result && enforce) {
                logger.error("Could not remove pending {} ", e.getIdentifier());
            }
        }
    }

    @Override
    public void run() {
        initThreadName(this.getClass().getSimpleName());
        while (!doShutdown()) {
            setRunning();
            while (sortedPending.size() > 0 && sortedPending.first().getSchedulingTime().isBefore(getNow())) {
                PendingEntry pendingTask = sortedPending.first();
                sortedPending.remove(pendingTask);
                logger.info("Found Pending: {}", pendingTask);
                try {
                    pendingTask.getPendingTask().run();
                } catch (Throwable t) {
                    logger.error(String.format("Executing PendingTask: %s Exception:", pendingTask.getIdentifier()), t);
                }
            }
            Duration toWait;
            if(sortedPending.size() > 0) {
                PendingEntry nextTask = sortedPending.first();
                toWait = Duration.between(node.getNow(), nextTask.getSchedulingTime()).plusMillis(1);
            }
            else {
                toWait = Duration.ofMillis(10000);
            }
            if(!toWait.isNegative()) {
                try {
                    final long toWaitTime = toWait.toMillis();
                    logger.info("Waiting {} milliseconds", toWaitTime);
                    if (toWaitTime > 0) {
                        synchronized (this) {
                            this.wait(toWait.toMillis());
                        }
                    }
                    logger.info("Awakened");
                } catch (InterruptedException e) {
                    if (!doShutdown())
                        logger.error("PendingHandler N: {} got interrupted {}", node.getUniqueNodeId(), e);
                    else
                        logger.info("PendingHandler N: {} got interrupted {}", node.getUniqueNodeId(), e);
                }
            }
        }
    }

    public void scheduleTaskHeartbeatOnNode(Task task) {
        Instant now = node.getNow();
        Duration claimedSignalPeriod = task.getDefinition().getClaimedSignalPeriod();
        String identifier = heartbeatSignallerName(task);
        Instant nextCall = now.plus(claimedSignalPeriod);
        final PendingEntry e = new PendingEntry(nextCall, identifier, new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternal(task, HEARTBEAT_I);
            }
        });
        schedulePending(e);
    }



    Random random = new Random();

    public void scheduleTaskForInitiation(final Task task, Duration toWait) {
        Instant nextCall = getNow().plus(toWait);
        final PendingEntry e = new PendingEntry(nextCall, task.getDefinition().getName() + "_ClaimingStarter", new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternal(task, NEW_I);
            }
        });
        schedulePending(e);
    }

    public void scheduleNodeHeartBeat(final Runnable runnable, final Instant nextCall) {
        final PendingEntry e = new PendingEntry(nextCall, "NodeHeartbeat", runnable);
        schedulePending(e);
    }

    public void scheduleTaskForClaiming(final Task task) {
        final int bound = (int) task.getDefinition().getPeriod().toMillis();
        long toWait = random.nextInt(bound);

        Instant nextCall = getNow().plus(Duration.ofMillis(toWait));
        final PendingEntry e = new PendingEntry(nextCall, task.getDefinition().getName() + "_ClaimingStarter", new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternal(task, CLAIMING_I);
            }
        });
        schedulePending(e);
    }


    public void scheduleTaskHandlingOnNode(Task task) {
        Instant initialTs = task.getDefinition().getInitialTimestamp();
        Instant nextCall;
        if (initialTs != null) {
            Instant now = node.getNow();
            Duration diff = Duration.between(initialTs, now);
            long number = diff.dividedBy(task.getDefinition().getPeriod());
            nextCall = initialTs.plus(task.getDefinition().getPeriod().multipliedBy(number + 1));
        } else {
            nextCall = node.getNow().plus(task.getDefinition().getPeriod());
        }
        final PendingEntry e = new PendingEntry(nextCall, taskStarterName(task), new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternal(task, HANDLING_I);
            }
        });
        schedulePending(e);
    }

    void scheduleTaskResurrection(final Task task) {
        Instant now = node.getNow();
        Duration diff = Duration.between(task.getLastClaimedInfo(), now);
        Instant nextCall = task.getLastClaimedInfo().plus(task.getDefinition().getClaimedSignalPeriod().multipliedBy(3));
        final PendingEntry e = new PendingEntry(nextCall, resurrectionName(task), new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternal(task, RESURRECTING);
            }
        });
        schedulePending(e);
    }

    void scheduleInterupter(final Task task, final String threadName, final Thread thread) {
        Instant now = node.getNow();
        Instant nextCall = now.plus(task.getDefinition().getMaxDuration());
        final PendingEntry e = new PendingEntry(nextCall, task.getDefinition().getName() + "_" + threadName, new Runnable() {
            @Override
            public void run() {
                if (thread.isAlive() && thread.getName().equals(threadName) && !thread.isInterrupted()) {
                    thread.interrupt();
                }
            }
        });
        schedulePending(e);


    }

    private Instant getNow() {
        return Instant.now(node.getClock());
    }

    public void removeClaimedSignaller(final Task t) {
        removePending(heartbeatSignallerName(t),false);
    }

    public void removeTaskResurrection(final Task t) {
        removePending(resurrectionName(t),true);
    }


    public void removeTaskStarter(final Task t) {
        removePending(taskStarterName(t), false);
    }

    private static String heartbeatSignallerName(final Task task) {
        return task.getDefinition().getName() + "_" + "ClaimedSignaler";
    }

    private static String resurrectionName(final Task task) {
        return task.getDefinition().getName() + "_Resurrection";
    }

    private static String taskStarterName(final Task task) {
        return task.getDefinition().getName() + "_" + "TaskStarter";
    }



}
