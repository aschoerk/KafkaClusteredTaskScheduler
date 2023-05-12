package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.CLAIMING;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.INITIATING;
import static net.oneandone.kafka.clusteredjobs.api.TaskStateEnum.NEW;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.Node;
import net.oneandone.kafka.clusteredjobs.api.NodeInformation;
import net.oneandone.kafka.clusteredjobs.api.TaskStateEnum;

/**
 * @author aschoerk
 */
public class NodeInformationHandler {

    Logger logger = LoggerFactory.getLogger(NodeInformationHandler.class);

    boolean firstHeartbeatArrived = false;

    final NodeImpl node;

    Set<String> nodesSeen = new HashSet<>();

    Set<String> nodesReceivedFrom = new HashSet<>();

    Map<String, NodeInformation> lastNodeInformations = new HashMap<>();

    public NodeInformationHandler(final NodeImpl node) {
        this.node = node;
    }

    boolean unsureState(Task task) {
        TaskStateEnum state = task.getLocalState();
        return state == NEW || state == ERROR;
    }

    /**
     * Interpret NodeInformation to check with local information about tasks
     * if remote state is:
     * NEW, INITIATING, CLAIMING: currently starting up task, no further interpretation
     * CLAIMED_BY_OTHER: check claiming node
     *  if it is me: CLAIMED_BY_NODE should be the state of the task here
     *  if it is unknown: could be me before restarting or could be node not having sent a NodeHeartbeat yet
     *  if it is known not me: CLAIMED_BY_OTHER as defined here
     * HANDLING_BY_OTHER: check claiming node
     *  if it is me: HANDLING_BY_NODE should be the state of the task here
     *  if it is unknown: could be me before restarting or could be node not having sent a NodeHeartbeat yet
     *  if it is known not me: CLAIMED_BY_OTHER as defined here
     * CLAIMED_BY_NODE, HANDLING_BY_NODE: should not contradict my entries
     *   CLAIMED_BY_OTHER, HANDLING_BY_OTHER with sending node
     *   NEW, ERROR means no data arrived yet about it correct by setting CLAIMED_BY_OTHER, HANDLING_BY_OTHER
     * ERROR: no data to be extracted from that
     *
     * @param nodeInformation
     */
    public void handle(NodeInformation nodeInformation) {
        if (nodeInformation.getName().equals(node.getUniqueNodeId())) {
            if (!firstHeartbeatArrived) {
                firstHeartbeatArrived = true;
                logger.info("Node: {} first  nodeheartbeat arrived", nodeInformation.getName());
            } else {
                logger.info("Node: {} second nodeheartbeat arrived", nodeInformation.getName());
                node.tasks.values().forEach(t -> {
                    if (t.getLocalState() == NEW) {
                        logger.info("Node: {} initiating task {}/{} in state NEW after second nodeheartbeat arrived from me",
                                node.getUniqueNodeId(), t.getDefinition().getName(), t.getLocalState());
                        t.setLocalState(INITIATING);
                        node.getPendingHandler().scheduleTaskForClaiming(t);
                    }
                });
            }

        } else {
            nodesSeen.add(nodeInformation.getName());
            nodesReceivedFrom.add(nodeInformation.getName());
            nodeInformation.getTaskInformation().forEach(ti -> {
                Task task = node.tasks.get(ti.getTaskName());
                if (task != null) {
                    if (task.getLocalState() == NEW) {
                        switch (ti.getState()) {
                            case NEW:
                            case INITIATING:
                            case CLAIMING:
                            case ERROR:
                                break;
                            case HANDLING_BY_NODE:
                                if(unsureState(task)) {
                                    task.setLocalState(TaskStateEnum.HANDLING_BY_OTHER, nodeInformation.getName());
                                }
                                break;
                            case CLAIMED_BY_NODE:
                                if(unsureState(task)) {
                                    task.setLocalState(TaskStateEnum.CLAIMED_BY_OTHER, nodeInformation.getName());
                                }
                                break;
                            case HANDLING_BY_OTHER:
                            case CLAIMED_BY_OTHER:
                                String supposedClaimingNode = ti.getNodeName().get();
                                nodesSeen.add(supposedClaimingNode);
                                if(supposedClaimingNode.equals(node.getUniqueNodeId())) {
                                    if(task.getLocalState() != TaskStateEnum.HANDLING_BY_NODE && ti.getState() == TaskStateEnum.HANDLING_BY_OTHER
                                       || task.getLocalState() != TaskStateEnum.CLAIMED_BY_NODE && ti.getState() == TaskStateEnum.CLAIMED_BY_OTHER) {
                                        logger.warn("Discrepancy in nodeHeartBeatStates: {},{} and local {}", nodeInformation.getName(), ti, task);
                                    }
                                }
                                break;
                        }
                    }
                } else {
                    logger.info("Task {}/{} not found on node: {}", ti.getTaskName(),
                            ti.getNodeName().orElse("unknown"), node.getUniqueNodeId());
                }
            });
        }
    }
}
