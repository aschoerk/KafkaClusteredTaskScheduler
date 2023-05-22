package net.oneandone.kafka.clusteredjobs.states;

import net.oneandone.kafka.clusteredjobs.NodeImpl;

/**
 * The state of a Task on a node
 */
public enum StateEnum implements StateInterface {

    /**
     * not in state-machine yet, waiting for more information from other node, if there is any
     */
    NULL {
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new Null(node);
        }
    },
    NEW {
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new New(node);
        }
    },
    /**
     * initiates the claiming
     */
    INITIATING{
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new Initiating(node);
        }
    },
    /**
     * after UNCLAIMED was sent, wait for the message to arrive before going into INITIATING
     */
    UNCLAIMING{
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new UnClaiming(node);
        }
    },
    /**
     * sent availability for executing the task
     */
    CLAIMING{
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new Claiming(node);
        }
    },
    /**
     * there is another node responsible for executing the task
     */
    CLAIMED_BY_OTHER{
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new ClaimedByOther(node);
        }
    },
    /**
     * this node feels responsible for executing the task
     */
    CLAIMED_BY_NODE{
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new ClaimedByNode(node);
        }
    },
    /**
     * claimed and currently executing by other nodes
     */
    HANDLING_BY_OTHER{
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new HandlingByOther(node);
        }
    },
    /**
     * claimed and currently executing by the node itself
     */
    HANDLING_BY_NODE{
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new HandlingByNode(node);
        }
    },
    /**
     * unexpected signals arrived
     */
    ERROR {
        @Override
        public StateHandlerBase createState(final NodeImpl node) {
            return new Error(node);
        }
    },
}
