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
    HEARTBEAT {
        // inform other nodes that task is claimed by this node in a periodical manner
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.heartbeatEvent(task, s);
        }
    },
    /**
     * This node is currently handling the task
     */
    HANDLING {
        // the node having claimed a task did start the handling. During that period no heartbeats-signals are sent.
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
            signalHandler.unclaimedEvent(task, s);
        }
    },
    /**
     * node signal after CLAIMING or in response to other CLAIMING if node has already claimed
     */
    CLAIMED {
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.claimedEvent(task, s);
        }
    },

    /**
     * used to provoke sending of NodeInformation
     */
    DOHEARTBEAT {
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.doheartbeat(task, s);
        }
    },

    /**
     * initiating a new task
     */
    INITIATING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.initiating_i(task, s);
        }
    },

    /**
     * start handling a claimed task
     */
    HANDLING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.handling_i(task, s);
        }
    },

    /**
     * Send indication that task is claimed in regular intervals. Otherwise th need for resurrection could be
     * indicated
     */
    HEARTBEAT_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.heartbeat_i(task, s);
        }
    },

    /**
     * after some time in state INITIATING start CLAIMING
     */
    CLAIMING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.claiming_i(task, s);
        }
    },
    /**
     * release claim on task
     */
    UNCLAIM_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.unclaim_i(task, s);
        }
    },
    /**
     * indicate ready handling of a task
     */
    UNHANDLING_I {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.unhandling_i(task, s);
        }
    },
    /**
     * indicate starting to resurrect a task
     */
    RESURRECTING {
        public boolean isInternal() { return true; }
        @Override
        public void handle(SignalHandler signalHandler, final Task task, final Signal s) {
            signalHandler.resurrecting(task, s);
        }
    }
}
