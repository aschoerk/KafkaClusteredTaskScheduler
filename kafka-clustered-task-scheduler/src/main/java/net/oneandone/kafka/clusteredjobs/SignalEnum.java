package net.oneandone.kafka.clusteredjobs;

/**
 * Signals control the state-transitions.
 */
public enum SignalEnum implements SignalInterface {
    /**
     *  signal TaskImpl is initiated, who is able to execute it may try to claim it
     *  if multiple node do initiating, check if parameters
     */

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
    DOHEARTBEAT,

    /**
     * initiating a new task
     */
    INITIATING_I {
        public boolean isInternal() { return true; }

    },

    /**
     * start handling a claimed task
     */
    HANDLING_I {
        public boolean isInternal() { return true; }

    },

    /**
     * Send indication that task is claimed in regular intervals. Otherwise th need for resurrection could be
     * indicated
     */
    HEARTBEAT_I {
        public boolean isInternal() { return true; }

    },

    /**
     * after some time in state INITIATING start CLAIMING
     */
    CLAIMING_I {
        public boolean isInternal() { return true; }

    },
    /**
     * release claim on task
     */
    UNCLAIM_I {
        public boolean isInternal() { return true; }

    },
    /**
     * indicate ready handling of a task
     */
    UNHANDLING_I {
        public boolean isInternal() { return true; }

    },
    /**
     * indicate starting to resurrect a task
     */
    RESURRECTING {
        public boolean isInternal() { return true; }

    };

    @Override
    public String toString() {
        return this.name();
    }
}
