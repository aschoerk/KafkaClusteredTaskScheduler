package net.oneandone.kafka.clusteredjobs;

import static org.apache.logging.log4j.Level.ERROR;
import static org.apache.logging.log4j.Level.WARN;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

/**
 * @author aschoerk
 */
@Plugin(name = "InterceptingAppender", category = "Core", elementType = "appender", printObject = true)
public class InterceptingAppender extends AbstractAppender {
    private Appender consoleAppender = null;
    protected InterceptingAppender(String name, Filter filter, Layout<?> layout, boolean ignoreExceptions) {
        super(name, filter, layout, ignoreExceptions);

    }
    @PluginFactory
    public static InterceptingAppender createAppender() {
        return new InterceptingAppender("InterceptingAppender", null, null, false );
    }

    public static AtomicLong countWarnings = new AtomicLong();
    public static AtomicLong countErrors = new AtomicLong();
    public static AtomicLong countElse = new AtomicLong();
    @Override
    public void append(final LogEvent event) {
        if (consoleAppender == null) {
            LoggerContext context = LoggerContext.getContext();
            consoleAppender = context.getConfiguration().getAppender("Console");
        }
        if (event.getLoggerName().startsWith(this.getClass().getPackageName())) {
            if (event.getLevel().equals(ERROR)) {
                countErrors.incrementAndGet();
            } else if (event.getLevel().equals(WARN)) {
                countWarnings.incrementAndGet();
            } else {
                 countElse.incrementAndGet();
            }
        }
        consoleAppender.append(event);
    }
}
