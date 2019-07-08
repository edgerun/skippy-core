import logging
from itertools import islice, cycle
from operator import itemgetter, add
from typing import List, Tuple

from core.clustercontext import ClusterContext
from core.model import Pod, Node, SchedulingResult
from core.predicates import Predicate, GeneralPreds, PodFitsResourcesPred
from core.priorities import Priority, EqualPriority, BalancedResourcePriority, \
    LatencyAwareImageLocalityPriority, CapabilityPriority, DataLocalityPriority, LocalityTypePriority


class Scheduler:

    # Context containing all the cluster information
    cluster_context: ClusterContext

    # Needs to contain all predicates that should be executed (if they're not overwritten in the constructor)
    default_predicates: List[Predicate] = [PodFitsResourcesPred()]

    # Needs to contain all priorities that should be executed (if they're not overwritten in the constructor)
    default_priorities: List[Tuple[int, Priority]] = [(1, EqualPriority()),
                                                      (1, BalancedResourcePriority()),
                                                      (1, LatencyAwareImageLocalityPriority()),
                                                      (1, LocalityTypePriority()),
                                                      (1, DataLocalityPriority()),
                                                      (1, CapabilityPriority())]

    # Defines at which index the last scoring stopped (i.e. where the next one should start)
    last_scored_node_index = 0

    def __init__(self, cluster_context: ClusterContext, percentage_of_nodes_to_score: int = 100,
                 predicates: List[Predicate] = None,
                 priorities: List[Tuple[int, Priority]] = None):
        if priorities is None:
            priorities = self.default_priorities
        if predicates is None:
            predicates = self.default_predicates

        self.predicates = predicates
        self.priorities = priorities
        self.percentage_of_nodes_to_score = percentage_of_nodes_to_score
        self.cluster_context = cluster_context

    def schedule(self, pod: Pod) -> SchedulingResult:
        """
        Decides on which node to place a pod.

        :param pod: to place
        :return: str name of the node to place the pod on
        """
        logging.debug('Received a new pod to schedule: %s', pod.name)

        nodes = self.cluster_context.list_nodes()
        # How many are <percentage_of_nodes_to_score>%?
        # https://kubernetes.io/docs/concepts/configuration/scheduler-perf-tuning/#tuning-percentageofnodestoscore
        # https://github.com/kubernetes/kubernetes/blob/a352b74bcca9de2acd1600e47ea7f122ac8c1fe1/pkg/scheduler/core/generic_scheduler.go#L435
        num_of_nodes_to_score = int(len(nodes) / 100 * self.percentage_of_nodes_to_score)

        # Find feasible nodes in the nodes to score (round robin beginning at the last stop)
        node_slice = islice(cycle(nodes), self.last_scored_node_index,
                            self.last_scored_node_index + num_of_nodes_to_score)
        feasible_nodes: [Node] = list(filter(lambda node: self.passes_predicates(pod, node), node_slice))

        # If less than 50 feasible nodes were found, the rest of the nodes are scored as well
        if len(feasible_nodes) < 50:
            node_slice = islice(cycle(nodes), self.last_scored_node_index + num_of_nodes_to_score,
                                self.last_scored_node_index + len(nodes))
            feasible_nodes.extend(list(filter(lambda node: self.passes_predicates(pod, node), node_slice)))
            # All nodes were evaluated
            evaluated_nodes = len(nodes)
            self.last_scored_node_index = self.last_scored_node_index + len(nodes) % len(nodes)
        else:
            # Only a percentage has been evaluated
            evaluated_nodes = num_of_nodes_to_score
            self.last_scored_node_index = (self.last_scored_node_index + num_of_nodes_to_score) % len(nodes)

        # Score all feasible nodes
        # Possible: The generic_scheduler.go parallelizes the score calculation (map reduce pattern)
        # We could just use multiprocessing.Pool()'s map function?
        # https://stackoverflow.com/questions/1704401/is-there-a-simple-process-based-parallel-map-for-python
        # TODO this loop could be heavily optimized, especially when removing the priority reduction step
        scored_nodes: [int] = [0] * len(feasible_nodes)
        for weighted_priority in self.priorities:
            weight = weighted_priority[0]
            priority = weighted_priority[1]
            mapped_nodes = [priority.map_node_score(self.cluster_context, pod, node) for node in feasible_nodes]
            reduced_node_scores = priority.reduce_mapped_score(self.cluster_context, pod, feasible_nodes, mapped_nodes)
            weighted_node_scores = [score * weight for score in reduced_node_scores]
            scored_nodes = list(map(add, weighted_node_scores, scored_nodes))
            logging.debug(f'Pod {pod.name} / {type(priority).__name__}: {weighted_node_scores}')
        scored_named_nodes: [(Node, int)] = list(zip(feasible_nodes, scored_nodes))

        logging.debug(f'Node scores: {scored_named_nodes}')

        # Find the name of the node with the highest score or None
        sorted_scored_nodes = max(scored_named_nodes, key=itemgetter(1), default=(None, 0))
        suggested_host: Node = next(iter(sorted_scored_nodes), None)
        suggested_host_name = None if suggested_host is None else suggested_host.name

        if suggested_host is not None:
            self.cluster_context.place_pod_on_node(pod, suggested_host)
            logging.debug('Found best node. Remaining allocatable resources after scheduling: %s',
                          suggested_host.allocatable)

        return SchedulingResult(suggested_host=suggested_host_name, evaluated_nodes=evaluated_nodes,
                                feasible_nodes=len(feasible_nodes))

    def passes_predicates(self, pod: Pod, node: Node) -> bool:
        # Conjunction over all node predicate checks
        return all(self.__passes_and_logs_predicate(predicate, self.cluster_context, pod, node)
                   for predicate in self.predicates)

    # noinspection PyMethodMayBeStatic
    def __passes_and_logs_predicate(self, predicate: Predicate, context: ClusterContext, pod: Pod, node: Node):
        result = predicate.passes_predicate(context, pod, node)
        logging.debug(f'Pod {pod.name} / Node {node.name} / {type(predicate).__name__}: '
                      f'{"Passed" if result else "Failed"}')
        return result
