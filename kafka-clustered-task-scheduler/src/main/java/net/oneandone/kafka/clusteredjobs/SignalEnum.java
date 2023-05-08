package net.oneandone.kafka.clusteredjobs;

/**
 * Signals control the state-transitions.
 */
public enum SignalEnum implements SignalInterface {
    /**
     *  signal Task is initiated, who is able to execute it may try to claim it
     *  if multiple node do initiating, check if parameters
     */
    /**
     * starting to initiate Task on this node
     */
    INITIATING {
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.initiatingEvent(task, s);
        }
    },
    /**
     * This node is prepared to CLAIM the task
     */
    CLAIMING{
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.claimingEvent(task, s);
        }
    },   //
    /**
     * This node CLAIMED the task
     */
    CLAIMED {
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.claimedEvent(task, s);
        }
    },
    /**
     * This node is currently handling the task
     */
    HANDLING {
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.handlingEvent(task, s);
        }
    },
    /**
     * This node is unclaiming the task
     */
    UNCLAIMED {
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.unclaimEvent(task, s);
        }
    },
    CLAIMED_YET {
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.claimedYetEvent(task, s);
        }
    },

    INITIATING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.initiating_i(task, s);
        }
    },

    HANDLING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.handling_i(task, s);
        }
    },

    CLAIMED_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.claimed_i(task, s);
        }
    },

    CLAIMING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.claiming_i(task, s);
        }
    },
    UNCLAIM_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.unclaim_i(task, s);
        }
    },
    UNHANDLING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.unhandling_i(task, s);
        }
    },
    RESURRECTING {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.resurrecting(task, s);
        }
    }
}
