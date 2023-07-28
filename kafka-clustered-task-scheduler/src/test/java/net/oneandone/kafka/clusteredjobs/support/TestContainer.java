package net.oneandone.kafka.clusteredjobs.support;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.oneandone.kafka.clusteredjobs.api.Container;

/**
 * @author aschoerk
 */
public class TestContainer implements Container {


    private String syncTopicName;
    private String bootstrapServers;

    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    ExecutorService executorService;

    public TestContainer(final String syncTopicName, final String bootstrapServers) {
        this.syncTopicName = syncTopicName;
        this.bootstrapServers = bootstrapServers;
        executorService = new ThreadPoolExecutor(20, 50, 10000,
                TimeUnit.MILLISECONDS, workQueue);
    }

    @Override
    public String getSyncTopicName() {
        return syncTopicName;
    }

    @Override
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public Future submitInThread(Runnable runnable) {
        return executorService.submit(runnable);
    }
}
