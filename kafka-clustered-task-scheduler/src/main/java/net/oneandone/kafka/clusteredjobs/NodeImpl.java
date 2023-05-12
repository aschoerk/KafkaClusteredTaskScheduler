package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.SignalEnum.DOHEARTBEAT;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.Container;
import net.oneandone.kafka.clusteredjobs.api.NodeInformation;
import net.oneandone.kafka.clusteredjobs.api.TaskDefinition;

/**
 * @author aschoerk
 */
public class NodeImpl extends StoppableBase implements net.oneandone.kafka.clusteredjobs.api.Node {

    static final long CONSUMER_POLL_TIME = 500L;
    Logger logger = LoggerFactory.getLogger(NodeImpl.class);
    public static final long MAX_AGE_OF_SIGNAL = 60 * 1000;
    private static AtomicInteger nodeCounter = new AtomicInteger(0);

    private ArrayList<Stoppable> stoppables = new ArrayList<>();

    private Clock clock = Clock.systemDefaultZone();

    private int nodeId;

    final String syncTopic;
    Logger log = LoggerFactory.getLogger(NodeImpl.class);

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
    private Container container;
    private NodeHeartbeat nodeHeartbeat;
    private NodeInformationHandler nodeInformationHandler;

    public NodeImpl(Container container) {
        this.container = container;
        this.syncTopic = container.getSyncTopicName();
        this.nodeId = nodeCounter.incrementAndGet();
        this.bootstrapServers = container.getBootstrapServers();
        try {
            hostname = Inet4Address.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new KctmException("NodeImpl cannot identify host", e);
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
        if (isRunning())
            return;
        nodeInformationHandler = new NodeInformationHandler(this);
        signalsReceivingThread = getContainer().createThread(() ->
        {
            Stoppable s = new SignalsWatcher(this);
            stoppables.add(s);
            s.run();
            logger.info("stopped");
        }
        );
        signalsReceivingThread.start();

        pendingHandlerThread = getContainer().createThread(() ->
        {
            stoppables.add(getPendingHandler());
            getPendingHandler().run();
            logger.info("stopped");
        }
        );
        pendingHandlerThread.start();
        setRunning();
        while (!threadsRunning()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new KctmException("Interrupted while waiting for node startup",e);
            }
        }
        this.nodeHeartbeat = NodeHeartbeat.NodeHeartBeatBuilder.aNodeHeartBeat().build();
        nodeHeartbeat.getJob(this).run();
        getPendingHandler().schedulePending(new PendingEntry(getNow().plus(Duration.ofMillis(1000L)),"doHeartBeat" + getUniqueNodeId(),
                        () -> NodeImpl.this.getSender().sendSynchronous(null, DOHEARTBEAT)));
    }

    public NodeHeartbeat getNodeHeartbeat() {
        return nodeHeartbeat;
    }

    public String getUniqueNodeId() {
        return processName + "_" + nodeId;
    }

    public Task register(TaskDefinition taskDefinition) {
        if (!isRunning()) {
            throw new KctmException("trying to register in not running node");
        } else {
            while (!threadsRunning()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new KctmException("Interrupted while waiting for node startup",e);
                }
            }
        }
        Task task = new Task(taskDefinition);
        task.setNode(this);
        this.tasks.put(taskDefinition.getName(), task);
        getSignalHandler().handleInternal(task, SignalEnum.INITIATING_I);
        return task;
    }

    void releaseAllTasks() {
        tasks.values().forEach(t -> {
            getSignalHandler().handleInternal(t, SignalEnum.UNCLAIM_I);
        });
    }

    public Thread newHandlerThread(final Runnable runnable) {
        Thread result = getContainer().createThread((runnable));
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
            getSignalHandler().handleInternal(e.getValue(), SignalEnum.UNCLAIM_I);
        });
        try {
            Thread.sleep(NodeImpl.CONSUMER_POLL_TIME + 1000);
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

    public Task getTask(String taskName) {
        return this.tasks.get(taskName);
    }

    public Container getContainer() {
        return container;
    }

    @Override
    public NodeInformation getNodeInformation() {
        NodeInformationImpl result = new NodeInformationImpl(getUniqueNodeId());
        tasks.entrySet().forEach(e -> {
            Task task = e.getValue();
            result.addTaskInformation(new NodeInformationImpl.TaskInformationImpl(task));
        });
        return result;
    }

    public boolean threadsRunning() {
        return stoppables.stream().filter(s -> !s.isRunning()).findAny().isEmpty();
    }

    void sendHeartbeat() {
        getSender().getSyncProducer().send(new ProducerRecord(syncTopic, getUniqueNodeId(),
                KbXStream.jsonXStream.toXML(getNodeInformation())));
    }

    public NodeInformationHandler getNodeInformationHandler() {
        return nodeInformationHandler;
    }
}
