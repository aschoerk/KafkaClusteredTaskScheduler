package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.CLAIMED_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.CLAIMING_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.HANDLING_I;
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

    private final Node node;

    public PendingHandler(final Node node) {
        this.node = node;
    }

    @Override
    public void run() {
        initThreadName(this.getClass().getSimpleName());
        while (!doShutdown()) {
            while (sortedPending.size() > 0 && sortedPending.first().getSchedulingTime().isBefore(getNow())) {
                PendingEntry pendingTask = sortedPending.first();
                sortedPending.remove(pendingTask);
                logger.info("Found Pending: {}", pendingTask);
                try {
                    pendingTask.getPendingTask().run();
                } catch (Throwable t) {
                    logger.error("Executing PendingTask: {} Exception: {} ", pendingTask.getIdentifier(), t);
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
                    logger.info("Waiting {} milliseconds", toWait.toMillis());
                    synchronized (this) {
                        this.wait(toWait.toMillis());
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

    public void scheduleTaskClaimedOnNode(Task task) {
        Instant now = node.getNow();
        Duration claimedSignalPeriod = task.getClaimedSignalPeriod();
        String identifier = claimedSignallerName(task);
        Instant nextCall = now.plus(claimedSignalPeriod);
        final PendingEntry e = new PendingEntry(nextCall, identifier, new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handle(task, CLAIMED_I);
            }
        });
        schedulePending(e);
    }



    Random random = new Random();

    public void scheduleTaskForClaiming(final Task task) {
        long toWait = random.nextInt((int) task.getPeriod().toMillis());

        Instant nextCall = getNow().plus(Duration.ofMillis(toWait));
        final PendingEntry e = new PendingEntry(nextCall, task.getName() + "_ClaimingStarter", new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handle(task, CLAIMING_I);
            }
        });
        schedulePending(e);
    }


    public void scheduleTaskHandlingOnNode(Task task) {
        Instant initialTs = task.getInitialTimestamp();
        Instant now = node.getNow();
        Duration diff = Duration.between(initialTs, now);
        long number = diff.dividedBy(task.getPeriod());
        Instant nextCall = initialTs.plus(task.getPeriod().multipliedBy(number + 1));
        final PendingEntry e = new PendingEntry(nextCall, taskStarterName(task), new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handle(task, HANDLING_I);
            }
        });
        schedulePending(e);
    }

    void scheduleTaskResurrection(final Task task) {
        Instant now = node.getNow();
        Duration diff = Duration.between(task.getLastClaimedInfo(), now);
        Instant nextCall = task.getLastClaimedInfo().plus(task.getClaimedSignalPeriod().multipliedBy(3));
        final PendingEntry e = new PendingEntry(nextCall, resurrectionName(task), new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handle(task, RESURRECTING);
            }
        });
        schedulePending(e);
    }


    private void schedulePending(final PendingEntry e) {
        logger.info("Scheduling PendingEntry: {}", e);
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

    private Instant getNow() {
        return Instant.now(node.getClock());
    }

    public void removeClaimedSignaller(final Task t) {
        removePending(claimedSignallerName(t),false);
    }

    public void removeTaskResurrection(final Task t) {
        removePending(resurrectionName(t),true);
    }


    public void removeTaskStarter(final Task t) {
        removePending(taskStarterName(t), false);
    }

    private static String claimedSignallerName(final Task task) {
        return task.getName() + "_" + "ClaimedSignaler";
    }

    private static String resurrectionName(final Task task) {
        return task.getName() + "_Resurrection";
    }

    private static String taskStarterName(final Task task) {
        return task.getName() + "_" + "TaskStarter";
    }


}
