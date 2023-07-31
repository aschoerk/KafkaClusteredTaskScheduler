package net.oneandone.kafka.clusteredjobs;

/**
 * Signals control the state-transitions.
 */
public enum SignalEnum implements SignalInterface {
    /**
     * This node is prepared to CLAIM the task
     */
    CLAIMING,   //
    /**
     * This node CLAIMED the task
     */
    HEARTBEAT ,
    /**
     * This node is currently handling the task
     */
    HANDLING,
    /**
     * This node is unclaiming the task
     */
    UNCLAIMED,
    /**
     * node signal after CLAIMING or in response to other CLAIMING if node has already claimed
     */
    CLAIMED,

    /**
     * used to provoke sending of NodeInformation
     */
    DO_INFORMATION_SEND,

    /**
     * make sure node informs that it is alive in minimal time-intervals
     */
    NODEHEARTBEAT,


    /**
     * initiating a new task
     */
    INITIATING_I {
        @Override
        public boolean isInternal() { return true; }

    },

    /**
     * start handling a claimed task
     */
    HANDLING_I {
        @Override
        public boolean isInternal() { return true; }

    },

    /**
     * Send indication that task is claimed in regular intervals. Otherwise th need for resurrection could be
     * indicated
     */
    HEARTBEAT_I {
        @Override
        public boolean isInternal() { return true; }

    },

    /**
     * after some time in state INITIATING start CLAIMING
     */
    CLAIMING_I {
        @Override
        public boolean isInternal() { return true; }

    },
    /**
     * release claim on task
     */
    UNCLAIM_I {
        @Override
        public boolean isInternal() { return true; }

    },
    /**
     * indicate ready handling of a task
     */
    UNHANDLING_I {
        @Override
        public boolean isInternal() { return true; }

    },
    /**
     * indicate starting to resurrect a task
     */
    REVIVING {
        @Override
        public boolean isInternal() { return true; }

    };

    @Override
    public String toString() {
        return this.name();
    }
}
