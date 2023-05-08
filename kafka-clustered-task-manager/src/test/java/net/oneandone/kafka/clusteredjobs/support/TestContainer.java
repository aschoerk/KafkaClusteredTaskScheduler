package net.oneandone.kafka.clusteredjobs.support;

import net.oneandone.kafka.clusteredjobs.api.Container;

/**
 * @author aschoerk
 */
public class TestContainer implements Container {


    private String syncTopicName;
    private String bootstrapServers;

    public TestContainer(final String syncTopicName, final String bootstrapServers) {
        this.syncTopicName = syncTopicName;
        this.bootstrapServers = bootstrapServers;
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
    public Thread createThread(Runnable runnable) {
        return new Thread(runnable);
    }
}
