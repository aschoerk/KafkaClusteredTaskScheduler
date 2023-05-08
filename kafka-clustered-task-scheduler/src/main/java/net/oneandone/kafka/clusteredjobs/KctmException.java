package net.oneandone.kafka.clusteredjobs;

/**
 * @author aschoerk
 */
public class KctmException extends RuntimeException {
    private static final long serialVersionUID = -6161894926827596801L;

    public KctmException(final String message) {
        super(message);
    }

    public KctmException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
