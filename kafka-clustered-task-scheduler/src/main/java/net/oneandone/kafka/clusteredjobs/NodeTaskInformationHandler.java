package net.oneandone.kafka.clusteredjobs;

import static net.oneandone.kafka.clusteredjobs.api.StateEnum.CLAIMED_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.ERROR;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.HANDLING_BY_NODE;
import static net.oneandone.kafka.clusteredjobs.api.StateEnum.NEW;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.clusteredjobs.api.NodeTaskInformation;
import net.oneandone.kafka.clusteredjobs.api.StateEnum;

/**
 * @author aschoerk
 */
public class NodeTaskInformationHandler {

    Logger logger = LoggerFactory.getLogger(NodeTaskInformationHandler.class);

    boolean firstHeartbeatArrived = false;

    final NodeImpl node;

    Set<String> nodesSeen = new HashSet<>();

    Set<String> nodesReceivedFrom = new HashSet<>();

    private Map<String, Signal> unknownTaskSignals =  Collections.synchronizedMap(new HashMap<String, Signal>());

    SignalsWatcher signalsWatcher = null;

    void setSignalsWatcher(final SignalsWatcher signalsWatcher) {
        this.signalsWatcher = signalsWatcher;
    }

    NodeTaskInformationHandler(final NodeImpl node) {
        this.node = node;
    }

    boolean unsureState(TaskImpl task) {
        StateEnum state = task.getLocalState();
        return state == NEW || state == ERROR;
    }

    /**
     * Interpret NodeTaskInformation to check with local information about tasks
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
     * @param nodeTaskInformation event which arrived via Sync-Topic
     */
    public void handle(NodeTaskInformation nodeTaskInformation) {
        if (!nodeTaskInformation.getName().equals(node.getUniqueNodeId())) {
            logger.info("Node: {} nodeheartbeat from {} arrived",node.getUniqueNodeId(), nodeTaskInformation.getName());
            nodesSeen.add(nodeTaskInformation.getName());
            nodesReceivedFrom.add(nodeTaskInformation.getName());
            nodeTaskInformation.getTaskInformation().forEach(ti -> {
                TaskImpl task = node.tasks.get(ti.getTaskName());
                if (task != null && ti.getState() != null) {
                    if (task.getLocalState() == NEW) {
                        switch (ti.getState()) {
                            case NEW:
                            case INITIATING:
                            case CLAIMING:
                            case ERROR:
                                break;
                            case HANDLING_BY_NODE:
                                if(unsureState(task)) {
                                    logger.info("Node: {} nodeheartbeat taskstate {}/{} from {} arrived",node.getUniqueNodeId(), ti.getTaskName(), ti.getState(), nodeTaskInformation.getName());
                                    task.setLocalState(StateEnum.HANDLING_BY_OTHER, nodeTaskInformation.getName());
                                }
                                break;
                            case CLAIMED_BY_NODE:
                                if(unsureState(task)) {
                                    logger.info("Node: {} nodeheartbeat taskstate {}/{} from {} arrived",node.getUniqueNodeId(), ti.getTaskName(), ti.getState(), nodeTaskInformation.getName());
                                    task.setLocalState(StateEnum.CLAIMED_BY_OTHER, nodeTaskInformation.getName());
                                }
                                break;
                            case HANDLING_BY_OTHER:
                                if(unsureState(task)) {
                                    logger.info("Node: {} nodeheartbeat taskstate {}/{} from {} arrived",node.getUniqueNodeId(), ti.getTaskName(), ti.getState(), nodeTaskInformation.getName());
                                    task.setLocalState(StateEnum.HANDLING_BY_OTHER, nodeTaskInformation.getName());
                                }
                                break;
                            case CLAIMED_BY_OTHER:
                                if(unsureState(task)) {
                                    logger.info("Node: {} nodeheartbeat taskstate {}/{} from {} arrived",node.getUniqueNodeId(), ti.getTaskName(), ti.getState(), nodeTaskInformation.getName());
                                    task.setLocalState(StateEnum.CLAIMED_BY_OTHER, nodeTaskInformation.getName());
                                }
                                break;
                        }
                    }
                } else {
                    unknownTaskSignal(nodeTaskInformation.getName(), ti, nodeTaskInformation.getArrivalTime().orElse(node.getNow()));
                }
            });
        }
    }

    private void unknownTaskSignal(final String sender, final NodeTaskInformation.TaskInformation ti, Instant timestamp) {
        switch (ti.getState()) {
            case CLAIMED_BY_NODE:
            case CLAIMED_BY_OTHER:
                unknownTaskSignals.put(ti.getTaskName(), new Signal(sender, ti.getTaskName(), SignalEnum.CLAIMED, timestamp, null));
                break;
            case HANDLING_BY_NODE:
            case HANDLING_BY_OTHER:
                unknownTaskSignals.put(ti.getTaskName(), new Signal(sender, ti.getTaskName(), SignalEnum.HANDLING, timestamp, null));
                break;
            default:
                Signal lastSignal = unknownTaskSignals.get(ti.getTaskName());
                if (lastSignal != null && lastSignal.nodeProcThreadId.equals(sender)) {
                    unknownTaskSignals.remove(ti.getTaskName());
                }
        }
    }

    /**
     * looking on the history of nodeinformation and old signals try to figure out, what the state of a new task in a new node is
     * @param taskname the name of the task to be initiated
     * @return the information about the last relevant signal received for the task.
     */
    public Optional<Pair<String, SignalEnum>> getUnknownTaskSignal(String taskname) {
        Optional<Pair<String, SignalEnum>> result = Optional.empty();
        List<Pair<NodeTaskInformation, NodeTaskInformation.TaskInformation>> nodeInformation =
                signalsWatcher.getLastNodeInformation()
                .entrySet()
                .stream()
                .flatMap(e -> e.getValue().getTaskInformation().stream()
                        .filter(ti -> ti.getState() == CLAIMED_BY_NODE || ti.getState() == HANDLING_BY_NODE)
                        .map(ti -> Pair.of(e.getValue(), ti))
                )
                .collect(Collectors.toList());

        List<Pair<String, SignalEnum>> claimingNodes = signalsWatcher.getUnmatchedSignals().stream()
                .filter(s -> s.taskName.equals(taskname))
                .filter(s -> s.signal == SignalEnum.CLAIMED || s.signal == SignalEnum.HANDLING || s.signal == SignalEnum.HEARTBEAT)
                .map(s -> Pair.of(s.nodeProcThreadId, s.signal))
                .collect(Collectors.toList());
        if (claimingNodes.isEmpty()) {
            List<Pair<String, SignalEnum>> oldSignals = signalsWatcher.getOldSignals().stream()
                    .filter(s -> s.taskName.equals(taskname))
                    .filter(s -> s.signal == SignalEnum.CLAIMED || s.signal == SignalEnum.HANDLING || s.signal == SignalEnum.HEARTBEAT)
                    .map(s -> Pair.of(s.nodeProcThreadId, s.signal))
                    .collect(Collectors.toList());
            if (oldSignals.size() > 0) {
                result = Optional.of(oldSignals.get(oldSignals.size() - 1));
            } else {
                if (nodeInformation.size() > 0) {
                    Pair<NodeTaskInformation, NodeTaskInformation.TaskInformation> res = nodeInformation.get(nodeInformation.size() - 1);
                    SignalEnum signal;
                    switch (res.getRight().getState()) {
                        case CLAIMED_BY_NODE:
                            signal = SignalEnum.CLAIMED;
                            break;
                        case HANDLING_BY_NODE:
                            signal = SignalEnum.HANDLING;
                            break;
                        default:
                            throw new KctmException("Logic error");
                    }
                    result = Optional.of(Pair.of(res.getLeft().getName(), signal));
                }
            }
        } else {
            result = Optional.of(claimingNodes.get(claimingNodes.size() - 1));
        }
        return result;
    }
}
