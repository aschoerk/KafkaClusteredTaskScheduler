package net.oneandone.kafka.clusteredjobs.support;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.oneandone.kafka.clusteredjobs.api.Configuration;
import net.oneandone.kafka.clusteredjobs.api.Container;

/**
 * @author aschoerk
 */
public class TestContainer implements Container {


    private final String syncTopicName;
    private final String bootstrapServers;

    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(50);


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
    public Future submitClusteredTaskThread(Runnable runnable) {
        return executorService.submit(runnable);
    }
    @Override
    public Future submitLongRunning(Runnable runnable) {
        return executorService.submit(runnable);
    }
    @Override
    public Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public Duration getNodeHeartBeat() {
                return Duration.ofSeconds(5);
            }

            @Override
            public Duration getReviverPeriod() {
                return Duration.ofSeconds(10);
            }
        };
    }
}
