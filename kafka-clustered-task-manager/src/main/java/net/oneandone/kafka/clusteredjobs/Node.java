package net.oneandone.kafka.clusteredjobs;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
public class Node implements Stoppable {

    static final long CONSUMER_POLL_TIME = 500L;
    Logger logger = LoggerFactory.getLogger(Node.class);
    public static final long CLAIMED_SEND_PERIOD = 30 * 1000;
    public static final long HANDLING_SEND_PERIOD = 10 * 1000;
    public static final long MAX_AGE_OF_SIGNAL = 60 * 1000;

    public static final long MAX_CLAIMING_TIME = 10 * 1000;

    private static AtomicInteger nodeCounter = new AtomicInteger(0);

    private ArrayList<Stoppable> stoppables = new ArrayList<>();

    private int nodeId;

    public final Sender sender;
    final String syncTopic;
    Logger log = LoggerFactory.getLogger(Node.class);

    final String SYNCING_GROUP = "SYNCING_GROUP";

    private final String hostname;
    private final String processName;
    private Thread signalsReceivingThread;
    private Thread watchingThread;
    final String bootstrapServers;

    ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();

    Integer taskPartition = null;


    public Node(String syncTopic, String bootstrapServers) {
        this.syncTopic = syncTopic;
        this.bootstrapServers = bootstrapServers;
        try {
            hostname = Inet4Address.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        processName = ManagementFactory.getRuntimeMXBean().getName();
        this.sender = new Sender( syncTopic, this);


    }

    public void run() {
        nodeId = nodeCounter.incrementAndGet();
        signalsReceivingThread = new Thread(() ->
        {
            Stoppable s = new SignalsWatcher(this, sender);
            stoppables.add(s);
            s.run();
            logger.info("stopped");
        }
        );
        signalsReceivingThread.start();

        watchingThread = new Thread(() ->
        {
            TaskWatcher s = new TaskWatcher(this, sender);
            stoppables.add(s);
            s.run();
            logger.info("stopped");
        }
        );
        watchingThread.start();
    }

    String getUniqueNodeId() {
        return hostname + "_" + processName + "_" + nodeId;
    }

    void register(Task task) {
        task.setLocalState(TaskStateEnum.UNCLAIM);
        tasks.put(task.getName(), task);
    }

    void releaseAllTasks() {
        tasks.values().forEach(t -> {
            if (t.getLocalState() == TaskStateEnum.CLAIMED_BY_NODE || t.getLocalState() == TaskStateEnum.HANDLING_BY_NODE) {
                t.setLocalState(TaskStateEnum.UNCLAIM);
            }
        });
    }

    @Override
    public void shutdown() {
        logger.info("Killing node: {}", getUniqueNodeId());
        stoppables.forEach(s -> s.shutdown());
        stoppables.clear();
        tasks.entrySet().forEach(e -> e.getValue().setLocalState(TaskStateEnum.UNCLAIM));
        try {
            Thread.sleep(Node.CONSUMER_POLL_TIME + 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        watchingThread.interrupt();
        signalsReceivingThread.interrupt();
        logger.info("Killed  node: {}", getUniqueNodeId());
    }
}
