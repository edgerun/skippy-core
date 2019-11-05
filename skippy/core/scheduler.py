import logging
from itertools import islice, cycle
from operator import itemgetter, add
from typing import List, Tuple

from core.clustercontext import ClusterContext
from core.model import Pod, Node, SchedulingResult
from core.predicates import Predicate, PodFitsResourcesPred
from core.priorities import Priority, BalancedResourcePriority, \
    LatencyAwareImageLocalityPriority, CapabilityPriority, DataLocalityPriority, LocalityTypePriority
from core.utils import normalize_image_name


class Scheduler:

    # Context containing all the cluster information
    cluster_context: ClusterContext

    # Needs to contain all predicates that should be executed (if they're not overwritten in the constructor)
    default_predicates: List[Predicate] = [PodFitsResourcesPred()]

    # Needs to contain all priorities that should be executed (if they're not overwritten in the constructor)
    default_priorities: List[Tuple[float, Priority]] = [(1.0, BalancedResourcePriority()),
                                                        (1.0, LatencyAwareImageLocalityPriority()),
                                                        (1.0, LocalityTypePriority()),
                                                        (1.0, DataLocalityPriority()),
                                                        (1.0, CapabilityPriority())]

    # Defines at which index the last scoring stopped (i.e. where the next one should start)
    last_scored_node_index = 0

    # https://github.com/kubernetes/kubernetes/blob/c1f40a5310b0abfe9a4fbddc24955360821a324b/pkg/scheduler/core/generic_scheduler.go#L58
    min_feasible_nodes_to_find = 100

    # https://github.com/kubernetes/kubernetes/blob/c1f40a5310b0abfe9a4fbddc24955360821a324b/pkg/scheduler/core/generic_scheduler.go#L63
    min_feasible_nodes_percentage_to_find = 5

    # https://github.com/kubernetes/kubernetes/blob/c1f40a5310b0abfe9a4fbddc24955360821a324b/pkg/scheduler/api/types.go#L40
    default_percentage_of_nodes_to_score = 50

    def __init__(self, cluster_context: ClusterContext, percentage_of_nodes_to_score: int = 100,
                 predicates: List[Predicate] = None,
                 priorities: List[Tuple[float, Priority]] = None):
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
        num_of_nodes_to_find = self.__num_feasible_nodes_to_find(len(nodes))

        filtered = filter(lambda node: self.passes_predicates(pod, node),
                          islice(cycle(nodes), self.last_scored_node_index, self.last_scored_node_index + len(nodes)))
        feasible_nodes: [Node] = list(islice(filtered, num_of_nodes_to_find))
        if len(feasible_nodes) > 0:
            self.last_scored_node_index = (nodes.index(feasible_nodes[-1]) + 1) % len(nodes)

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
        needed_images = None

        if suggested_host is not None:
            # Add a list of images needed to pull to the result (before manipulating the state with #place_pod_on_node
            needed_images = []
            for container in pod.spec.containers:
                if normalize_image_name(container.image) not in self.cluster_context.images_on_nodes[suggested_host.name]:
                    needed_images.append(normalize_image_name(container.image))

            self.cluster_context.place_pod_on_node(pod, suggested_host)
            logging.debug('Found best node. Remaining allocatable resources after scheduling: %s',
                          suggested_host.allocatable)

        return SchedulingResult(suggested_host=suggested_host, feasible_nodes=len(feasible_nodes),
                                needed_images=needed_images)

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

    # noinspection PyMethodMayBeStatic
    def __num_feasible_nodes_to_find(self, num_all_nodes: int) -> int:
        """
        Calculates the number of nodes which should be scored by the scheduler (changed in K8s 1.14):
        https://github.com/kubernetes/kubernetes/blob/c1f40a5310b0abfe9a4fbddc24955360821a324b/pkg/scheduler/core/generic_scheduler.go#L441
        https://kubernetes.io/docs/concepts/scheduling/scheduler-perf-tuning/#percentage-of-nodes-to-score
        :param num_all_nodes: total number of nodes
        :return: number of nodes the scheduler should score
        """
        if num_all_nodes < self.min_feasible_nodes_percentage_to_find or self.percentage_of_nodes_to_score >= 100:
            return num_all_nodes
        adaptive_percentage: float = self.percentage_of_nodes_to_score
        if adaptive_percentage <= 0:
            adaptive_percentage = self.default_percentage_of_nodes_to_score - num_all_nodes / 125
            if adaptive_percentage < self.min_feasible_nodes_percentage_to_find:
                adaptive_percentage = self.min_feasible_nodes_percentage_to_find
        num_nodes = int(num_all_nodes * adaptive_percentage / 100)
        if num_nodes < self.min_feasible_nodes_to_find:
            return self.min_feasible_nodes_to_find
        return num_nodes
