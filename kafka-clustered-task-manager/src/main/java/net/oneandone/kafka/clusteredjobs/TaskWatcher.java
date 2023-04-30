package net.oneandone.kafka.clusteredjobs;

import static java.lang.Math.random;
import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.CLAIMED_BY_ME;
import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.CLAIMING;
import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.HANDLED_BY_ME;
import static net.oneandone.kafka.clusteredjobs.TaskSignalEnum.UNCLAIMED;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
public class TaskWatcher extends StoppableBase {

    static Logger logger = LoggerFactory.getLogger(TaskWatcher.class);

    private final Node node;
    private final Sender sender;

    public TaskWatcher(Node node, Sender sender) {
        this.node = node;
        this.sender = sender;
    }

    public void run() {
        initThreadName(this.getClass().getSimpleName());
        while (!doShutdown()) {
            node.tasks.values().stream().forEach(t -> {
                if (t.getLocalState() != null) {
                    switch (t.getLocalState()) {
                        case CLAIMED_BY_NODE: {
                            t.sawClaimedInfo();
                            if(t.getLastStartup()== null || getNow().isAfter(t.getLastStartup().plus(t.getPeriod(), ChronoUnit.MILLIS))) {
                                t.setLocalState(TaskStateEnum.HANDLING_BY_NODE);
                                t.sawClaimedInfo();
                                new Thread(() -> {
                                    new TaskHandler(t,sender).run();
                                }).start();
                            }
                            else {
                                if(getNow().isAfter(t.getLastClaimedInfo().plus(Node.CLAIMED_SEND_PERIOD, ChronoUnit.MILLIS))) {
                                    sender.sendAsynchronous(t, CLAIMED_BY_ME);
                                }
                            }
                        }
                        break;
                        case HANDLING_BY_NODE: {
                            if(getNow().isAfter(t.getHandlingStarted().plus(node.HANDLING_SEND_PERIOD, ChronoUnit.MILLIS))) {
                                sender.sendAsynchronous(t, HANDLED_BY_ME);
                            }
                        }
                        break;
                        case INITIATING: {  // the claiming has been initiated
                            if(t.getClaimingTimestamp() == null) {
                                final long timeToWaitBeforeClaiming = (long) (random() * Node.MAX_CLAIMING_TIME);
                                logger.debug("Initiating {} on Node {}, waiting {} milliseconds before claiming", t.getName(), node.getUniqueNodeId(), timeToWaitBeforeClaiming);
                                t.setClaimingTimestamp(Instant.now(node.getClock()).plus(timeToWaitBeforeClaiming, ChronoUnit.MILLIS));
                            }
                            else {
                                if(getNow().isAfter(t.getClaimingTimestamp())) {
                                    t.setLocalState(TaskStateEnum.CLAIMING);
                                    sender.sendAsynchronous(t, CLAIMING);
                                    logger.debug("Claiming {} on Node {}", t.getName(), node.getUniqueNodeId());
                                    t.setClaimingTimestamp(null);
                                }
                            }
                        }
                        break;
                        case HANDLING_BY_OTHER:
                        case CLAIMED_BY_OTHER:  // no node sent information about having claimed a task for more than Node.MAX_AGE_OF_SIGNAL milliseconds.
                            if(getNow().isBefore(t.getLastClaimedInfo().plus(Node.MAX_AGE_OF_SIGNAL, ChronoUnit.MILLIS))) {
                                break;
                            }
                            ;
                        case UNCLAIM:  // node has explicitly unclaimed handling of a task
                            t.setLocalState(TaskStateEnum.INITIATING);
                            break;
                    }
                } else {
                    logger.info("Task {} unitialized yet", t);
                }
            });
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.info("Running for node: {} interrupted", node.getUniqueNodeId() );
                return;
            }

        }
        node.tasks.values().forEach(t -> {
            if (t.getLocalState() == TaskStateEnum.CLAIMED_BY_NODE || t.getLocalState() == TaskStateEnum.HANDLING_BY_NODE) {
                sender.sendAsynchronous(t, UNCLAIMED);
            }
        });
    }

    private Instant getNow() {
        return Instant.now(node.getClock());
    }
}
