package net.oneandone.kafka.clusteredjobs;

/**
 * Exception specific to kafka-clustered-task-scheduler
 */
public class KctmException extends RuntimeException {
    private static final long serialVersionUID = -6161894926827596801L;

    /**
     * {@inheritDoc}
     */
    public KctmException(final String message) {
        super(message);
    }

    /**
     * {@inheritDoc}
     */
    public KctmException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
