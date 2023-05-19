package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.HEARTBEAT_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.CLAIMING_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.HANDLING_I;
import static net.oneandone.kafka.clusteredjobs.SignalEnum.RESURRECTING;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.INITIATING;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.NEW;

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
 * during run() scheduled tasks represented by PendingEntry-Objects will be executed at their scheduled time.
 */
public class PendingHandler extends StoppableBase {

    private static final Logger logger = LoggerFactory.getLogger(PendingHandler.class);

    private final SortedSet<PendingEntry> sortedPending = Collections.synchronizedSortedSet(new TreeSet<>(new PendingEntry.TimestampComparator()));

    private final Map<String, PendingEntry> pendingByIdentifier = Collections.synchronizedMap(new HashMap<>());

    private final NodeImpl node;

    private final Random random = new Random();

    /**
     * create the single PendingHandler for a node
     * @param node the node the PendingHandler is created for
     */
    public PendingHandler(final NodeImpl node) {
        this.node = node;
    }


    /**
     * <b>The</b> scheduling function
     * @param e the pendingEntry to be scheduled
     */
    public void schedulePending(final PendingEntry e) {
        logger.info("Node: {} Scheduling PendingEntry: {}", node.getUniqueNodeId(), e);
        removePending(e.getIdentifier(), true);
        pendingByIdentifier.put(e.getIdentifier(), e);
        sortedPending.add(e);
        synchronized (this) {
            if(sortedPending.first().equals(e)) {
                this.notify();
            }
        }
    }


    /**
     * Remove a pendingEntry, if enfore is true log an error if it is gnerally found but no entry is currently scheduled
     * @param pendingName the identifier of the entry to be removed
     * @param enforce if true log an error if it is gnerally found but no entry is currently scheduled
     */
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

    /**
     * the runner executing the scheduled task. This is handled by waiting for the next task in a sorted Map of
     * PendingEntries using this.wait(). if a new tasks gets scheduled which must b handled earlier, notify will be called.
     */
    @Override
    public void run() {
        initThreadName(this.getClass().getSimpleName());
        while (!doShutdown()) {
            setRunning();
            while (sortedPending.size() > 0 && sortedPending.first().getSchedulingTime().isBefore(node.getNow())) {
                PendingEntry pendingTask = sortedPending.first();
                sortedPending.remove(pendingTask);
                logger.info("Found Pending: {}", pendingTask);
                try {
                    pendingTask.getPendingRunnable().run();
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

    /**
     * used to schedule the HEARTBEAT-Signal for claimed (not executing) tasks
     * @param task the task currently claimed, for which the HEARTBEAT signal is to be generated.
     */
    public void scheduleTaskHeartbeatOnNode(Task task) {
        Instant now = node.getNow();
        Duration claimedSignalPeriod = task.getDefinition().getClaimedSignalPeriod();
        String identifier = heartbeatSignallerIdentifier(task);
        Instant nextCall = now.plus(claimedSignalPeriod);
        final PendingEntry e = new PendingEntry(nextCall, identifier, new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternalSignal(task, HEARTBEAT_I);
            }
        });
        schedulePending(e);
    }


    /**
     * Schdule the periodic Message communicating the current state of the node
     * @param runnable the sending method
     * @param nextCall the timestamp when to do the next call.
     */
    public void scheduleNodeHeartBeat(final Runnable runnable, final Instant nextCall) {
        final PendingEntry e = new PendingEntry(nextCall, "NodeHeartbeat", runnable);
        schedulePending(e);
    }

    /**
     * wait for duration, after that switch state from NEW to INITIATING
     * @param duration max time to leave a task in state NEW
     */
    public void scheduleWaitForNewSwitch(Duration duration) {
        Instant nextCall = node.getNow().plus(duration);
        final PendingEntry e = new PendingEntry(nextCall, "NewSwitch",
                () -> {
                    logger.info("Node: {} switch of NEW Tasks", node.getUniqueNodeId());
                    node.tasks.values().forEach(t -> {
                        if (t.getLocalState() == NEW) {
                            logger.info("Node: {} initiating task {}/{} in state NEW after second nodeheartbeat arrived from me",
                                    node.getUniqueNodeId(), t.getDefinition().getName(), t.getLocalState());
                            t.setLocalState(INITIATING);
                            node.getPendingHandler().scheduleTaskForClaiming(t);
                        }
                    });
                });
        schedulePending(e);
    }

    /**
     * Schedule a random waiting time before trying to claim a task
     * @param task the task, currently in INITIATING state for which to wait before CLAIMING.
     */
    public void scheduleTaskForClaiming(final Task task) {
        final int bound = (int) task.getDefinition().getPeriod().toMillis();
        long toWait = random.nextInt(bound);

        Instant nextCall = node.getNow().plus(Duration.ofMillis(toWait));
        final PendingEntry e = new PendingEntry(nextCall, task.getDefinition().getName() + "_ClaimingStarter", new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternalSignal(task, CLAIMING_I);
            }
        });
        schedulePending(e);
    }


    /**
     * schedule the time until a claimed task is to be handled.
     * @param task the claimed task to be started
     */
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
        final PendingEntry e = new PendingEntry(nextCall, taskStarterIdentifier(task), new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternalSignal(task, HANDLING_I);
            }
        });
        schedulePending(e);
    }

    /**
     * Schedule a unclaimed task to be checked for resurrection.
     * @param task the task to be checked for HEARTBEAT signal received.
     */
    void scheduleTaskResurrection(final Task task) {
        Instant now = node.getNow();
        Duration diff = Duration.between(task.getLastClaimedInfo(), now);
        Instant nextCall = task.getLastClaimedInfo().plus(task.getDefinition().getClaimedSignalPeriod().multipliedBy(3));
        final PendingEntry e = new PendingEntry(nextCall, resurrectionIdentifier(task), new Runnable() {
            @Override
            public void run() {
                node.getSignalHandler().handleInternalSignal(task, RESURRECTING);
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

    /**
     * remove scheduled claimed-heartbeat generation
     * @param t the task for which the claimed-heartbeat is supposed to be scheduled
     */
    public void removeClaimedHeartbeat(final Task t) {
        removePending(heartbeatSignallerIdentifier(t),false);
    }

    /**
     * remove check for TaskResurrection.
     * @param t the task claimed by another node, to be checked if heart-beats arrived in time
     */
    public void removeTaskResurrection(final Task t) {
        removePending(resurrectionIdentifier(t),true);
    }


    /**
     * remove the scheduled starting of a task
     * @param t the task having been scheduled for starting
     */
    public void removeTaskStarter(final Task t) {
        removePending(taskStarterIdentifier(t), false);
    }

    private static String heartbeatSignallerIdentifier(final Task task) {
        return task.getDefinition().getName() + "_" + "ClaimedSignaler";
    }

    private static String resurrectionIdentifier(final Task task) {
        return task.getDefinition().getName() + "_Resurrection";
    }

    private static String taskStarterIdentifier(final Task task) {
        return task.getDefinition().getName() + "_" + "TaskStarter";
    }



}
