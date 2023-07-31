package net.oneandone.kafka.clusteredjobs;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PendingHandlerTest {

    @Test
    void canWakeupUsingNotify() throws InterruptedException {
        Clock fixed = Clock.fixed(Instant.now(), ZoneId.systemDefault() );
        NodeImpl node = Mockito.mock(NodeImpl.class);
        Mockito.when(node.getNow()).thenReturn(fixed.instant());
        PendingHandler peh = new PendingHandler(node);
        peh.setDefaultWaitMillis(Duration.ofMillis(1000000));

        Thread thread = new Thread(() -> peh.loopBody());
        thread.start();
        Thread.sleep(100);
        assertTrue(thread.isAlive(), "Thread should stay alive because of long waittime");
        int count = 0;
        while (thread.isAlive() && (count < 1000)) {
            synchronized (peh) {
                peh.notify();
            }
            Thread.sleep(10);
            count++;
        }
        assertTrue(count > 0, "should have done the notify at least once");
        assertFalse(thread.isAlive(), "even after 10 Seconds thread has not reacted to notify");
    }

    @Test
    void canScheduleNextTask() {
        Clock fixed = Clock.fixed(Instant.now(), ZoneId.systemDefault() );
        NodeImpl node = Mockito.mock(NodeImpl.class);
        Mockito.when(node.getNow()).thenReturn(fixed.instant());
        PendingHandler peh = new PendingHandler(node);
        peh.setDefaultWaitMillis(Duration.ofMillis(5));
        MutableBoolean called = new MutableBoolean(false);
        PendingEntry pe = new PendingEntry(fixed.instant().plus(Duration.ofMillis(1)), "test", () -> {
            called.setTrue();
        });
        peh.schedulePending(pe);
        peh.loopBody();
        Assertions.assertFalse(called.booleanValue(), "One millisecond to early");
        Mockito.when(node.getNow()).thenReturn(fixed.instant().plus(Duration.ofMillis(1)));
        peh.loopBody();
        assertTrue(called.booleanValue(), "should have executed at exact time");
        called.setValue(false);
        peh.loopBody();
        Assertions.assertFalse(called.booleanValue(), " should call entry only once");
        peh.schedulePending(pe);
        Mockito.when(node.getNow()).thenReturn(fixed.instant().plus(Duration.ofMillis(2)));
        peh.loopBody();
        assertTrue(called.booleanValue(), "should have executed at one millisecond later");
        called.setValue(false);
        peh.schedulePending(pe);
        peh.schedulePending(new PendingEntry(fixed.instant().plus(Duration.ofMillis(10)), "test", () -> {
            called.setTrue();
        }));
        peh.loopBody();
        Assertions.assertFalse(called.booleanValue(), "should have replaced pendingentry");
        Mockito.when(node.getNow()).thenReturn(fixed.instant().plus(Duration.ofMillis(10)));
        peh.loopBody();
        assertTrue(called.booleanValue(), "should have executed replaced entry");

        MutableInt incremented = new MutableInt(0);
        for (int i = 0; i < 200; i++) {
            peh.schedulePending(new PendingEntry(fixed.instant().plus(Duration.ofMillis(1)), "test" + i, () -> {
                incremented.increment();
            }));
        }
        peh.loopBody();
        assertEquals(200,incremented.getValue(), "should have executed all entries");


    }


}
