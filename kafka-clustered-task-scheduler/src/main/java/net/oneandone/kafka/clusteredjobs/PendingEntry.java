package net.oneandone.kafka.clusteredjobs;

import java.time.Instant;
import java.util.Comparator;

/**
 * future Task to be done by PendingHandler
 */
public class PendingEntry  {

    private Instant schedulingTime;

    final private Runnable pendingRunnable;

    final private String identifier;

    /**
     * Scheduled Task
     * @param schedulingTime time to execute the task
     * @param identifier the identifier unique to allow identifying and replacing
     * @param pendingRunnable the task to be executed at schedulingTime
     */
    public PendingEntry(Instant schedulingTime, String identifier, Runnable pendingRunnable) {
        this.schedulingTime = schedulingTime;
        this.pendingRunnable = pendingRunnable;
        this.identifier = identifier;
    }

    /**
     * return the time when the runnable is to be started
     * @return  the time when the runnable is to be started
     */
    public Instant getSchedulingTime() {
        return schedulingTime;
    }

    /**
     * return the runnable to be started at schedulingTime
     * @return the runnable to be started at schedulingTime
     */
    public Runnable getPendingRunnable() {
        return pendingRunnable;
    }

    /**
     * Return the identifier by which the unique pending entry can be found
     * @return  the identifier by which the unique pending entry can be found
     */
    public String getIdentifier() {
        return identifier;
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
        return identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    @Override
    public String toString() {
        return "PendingEntry{" +
               ", schedulingTime=" + schedulingTime +
               ", identifier='" + identifier + '\'' +
               '}';
    }


    /**
     * Comparator used to order PendingEntries in Treemap so the next to be done is always the first.
     */
    public static class TimestampComparator implements Comparator<PendingEntry> {
        @Override
        public int compare(final PendingEntry o1, final PendingEntry o2) {
            int result = o1.schedulingTime.compareTo(o2.schedulingTime);
            return result != 0 ? result : o1.identifier.compareTo(o2.identifier);
        }
    }
}
