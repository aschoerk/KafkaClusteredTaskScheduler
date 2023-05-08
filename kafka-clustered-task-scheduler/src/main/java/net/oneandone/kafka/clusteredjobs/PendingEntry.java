package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * future Task to be done by PendingHandler
 */
public class PendingEntry implements Comparable<PendingEntry> {

    static AtomicLong idFactory = new AtomicLong(1L);

    final Long id = idFactory.getAndIncrement();

    private Instant schedulingTime;

    final private Runnable pendingTask;

    final private String identifier;

    public PendingEntry(Instant schedulingTime, String identifier, Runnable pendingTask) {
        this.schedulingTime = schedulingTime;
        this.pendingTask = pendingTask;
        this.identifier = identifier;
    }
    public Instant getSchedulingTime() {
        return schedulingTime;
    }

    public Runnable getPendingTask() {
        return pendingTask;
    }

    public String getIdentifier() {
        return identifier;
    }

    public Long getId() {
        return id;
    }
    @Override
    public int compareTo(final PendingEntry o) {
        return schedulingTime.compareTo(o.schedulingTime);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        PendingEntry that = (PendingEntry) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "PendingEntry{" +
               "id=" + id +
               ", schedulingTime=" + schedulingTime +
               ", identifier='" + identifier + '\'' +
               '}';
    }

    public static class TimestampComparator implements Comparator<PendingEntry> {
        @Override
        public int compare(final PendingEntry o1, final PendingEntry o2) {
            int result = o1.schedulingTime.compareTo(o2.schedulingTime);
            return result != 0 ? result : o1.id.compareTo(o2.id);
        }
    }
}
