package net.oneandone.kafka.clusteredjobs;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
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

    private Clock clock = Clock.systemDefaultZone();

    private int nodeId;

    final String syncTopic;
    Logger log = LoggerFactory.getLogger(Node.class);

    final String SYNCING_GROUP = "SYNCING_GROUP";

    private final String hostname;
    private final String processName;
    private Thread signalsReceivingThread;
    private Thread pendingHandlerThread;
    final String bootstrapServers;

    ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();

    Integer taskPartition = null;
    private transient Sender sender;
    private transient SignalHandler signalHandler;
    private transient PendingHandler pendingHandler;

    private Set<Thread> handlerThreads = new HashSet<>();


    public Node(String syncTopic, String bootstrapServers) {
        this.syncTopic = syncTopic;
        this.bootstrapServers = bootstrapServers;
        try {
            hostname = Inet4Address.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new KctmException("Node cannot identify host", e);
        }
        processName = ManagementFactory.getRuntimeMXBean().getName();
    }

    Sender getSender() {
        if(sender == null) {
            synchronized (this) {
                if(sender == null) {
                    sender = new Sender(this);
                }
            }
        }
        return sender;
    }

    SignalHandler getSignalHandler() {
        if(signalHandler == null) {
            synchronized (this) {
                if(signalHandler == null) {
                    signalHandler = new SignalHandler(this);
                }
            }
        }
        return signalHandler;
    }

    PendingHandler getPendingHandler() {
        if(pendingHandler == null) {
            synchronized (this) {
                if(pendingHandler == null) {
                    pendingHandler = new PendingHandler(this);
                }
            }
        }
        return pendingHandler;
    }

    public void run() {
        nodeId = nodeCounter.incrementAndGet();
        signalsReceivingThread = new Thread(() ->
        {
            Stoppable s = new SignalsWatcher(this);
            stoppables.add(s);
            s.run();
            logger.info("stopped");
        }
        );
        signalsReceivingThread.start();

        pendingHandlerThread = new Thread(() ->
        {
            stoppables.add(getPendingHandler());
            getPendingHandler().run();
            logger.info("stopped");
        }
        );
        pendingHandlerThread.start();
    }

    String getUniqueNodeId() {
        return hostname + "_" + processName + "_" + nodeId;
    }

    void register(Task task) {
        getSignalHandler().handle(task, SignalEnum.INITIATING_I);
    }

    void releaseAllTasks() {
        tasks.values().forEach(t -> {
            getSignalHandler().handle(t, SignalEnum.UNCLAIM_I);
        });
    }
    public Thread newHandlerThread(final Runnable runnable) {
        Thread result = new Thread(runnable);
        handlerThreads.add(result);
        return result;
    }
    public void disposeHandlerThread(final Thread thread) {
        assert handlerThreads.remove(thread);
    }

    @Override
    public void shutdown() {
        logger.info("Killing node: {}", getUniqueNodeId());
        stoppables.forEach(s -> s.shutdown());
        stoppables.clear();
        tasks.entrySet().forEach(e -> {
            getSignalHandler().handle(e.getValue(), SignalEnum.UNCLAIM_I);
        });
        try {
            Thread.sleep(Node.CONSUMER_POLL_TIME + 1000);
        } catch (InterruptedException e) {
            throw new KctmException("During shutdown interrupted", e);
        }
        while (pendingHandlerThread.isAlive())
            pendingHandlerThread.interrupt();
        while (signalsReceivingThread.isAlive())
            signalsReceivingThread.interrupt();
        for (Thread t: handlerThreads) {
            while (t.isAlive()) {
                t.interrupt();
            }
        }
        logger.info("Killed  node: {}", getUniqueNodeId());
    }

    public Clock getClock() {
        return clock;
    }

    public void setClock(final Clock clockP) {
        this.clock = clockP;
    }

    public Instant getNow() {
        return Instant.now(clock);
    }

    // be able to override for testing purposes

    void setSender(final Sender senderP) {
        this.sender = senderP;
    }

    void setSignalHandler(final SignalHandler signalHandlerP) {
        this.signalHandler = signalHandlerP;
    }

    void setPendingHandler(final PendingHandler pendingHandlerP) {
        this.pendingHandler = pendingHandlerP;
    }
}
