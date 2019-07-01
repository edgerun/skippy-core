"""
Implementations of the kubernetes default scheduler's priority functions.
https://github.com/kubernetes/kubernetes/tree/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/scheduler/algorithm/priorities
"""
from math import fabs

from core.clustercontext import ClusterContext
from core.model import Pod, Node, Capacity, ImageState
from core.utils import normalize_image_name


class Priority:
    """ Abstract class for priority function implementations. """
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        """Calculates the score of a node for the pod"""
        raise NotImplementedError

    # noinspection PyMethodMayBeStatic
    def reduce_mapped_score(self, context: ClusterContext, pod: Pod, nodes: [Node], node_scores: [int]) -> [int]:
        """
        Reduces the already mapped scores of all nodes for a specific pod.
        The default implementation does not modify the scores anymore.
        """
        return node_scores


class EqualPriority(Priority):
    # noinspection PyMethodMayBeStatic
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        return 1


class ImageLocalityPriority(Priority):
    """https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/algorithm/priorities/image_locality.go"""
    mb: int = 1024 * 1024
    min_threshold: int = 23 * mb
    max_threshold: int = 1000 * mb

    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        return self.calculate_priority(context, self.sum_image_scores(context, pod, node))

    def calculate_priority(self, context: ClusterContext, sum_scores: int) -> int:
        if sum_scores < self.min_threshold:
            sum_scores = self.min_threshold
        elif sum_scores > self.max_threshold:
            sum_scores = self.max_threshold
        return int(context.max_priority * (sum_scores - self.min_threshold) / (self.max_threshold - self.min_threshold))

    def sum_image_scores(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        calc_sum = 0
        total_num_nodes = len(context.list_nodes())
        if pod.spec.containers is not Node:
            for container in pod.spec.containers:
                try:
                    image_state: ImageState = context.images_on_nodes[node.name][normalize_image_name(container.image)]
                    calc_sum += self.scaled_image_score(image_state, total_num_nodes)
                except KeyError:
                    pass
        return calc_sum

    # noinspection PyMethodMayBeStatic
    def scaled_image_score(self, image_state: ImageState, total_num_nodes: int) -> int:
        spread = float(image_state.num_nodes) / float(total_num_nodes)
        return int(float(image_state.size) * spread)


class ResourcePriority(Priority):
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        allocatable = node.allocatable
        requested = Capacity()
        requested.memory = 0
        requested.cpu_millis = 0
        requested.max_pods = 0
        for container in pod.spec.containers:
            requested.cpu_millis += container.resources.requests["cpu"]
            requested.memory += container.resources.requests["mem"]

        score = self.scorer(context, requested, allocatable)
        return score

    def scorer(self, context: ClusterContext, requested: Capacity, allocatable: Capacity):
        raise NotImplementedError


class BalancedResourcePriority(ResourcePriority):
    def scorer(self, context: ClusterContext, requested: Capacity, allocatable: Capacity):
        cpu_fraction = self.fraction_of_capacity(requested.cpu_millis, allocatable.cpu_millis)
        memory_fraction = self.fraction_of_capacity(requested.memory, allocatable.memory)

        # if requested >= capacity, the corresponding host should never be preferred.
        if cpu_fraction >= 1 or memory_fraction >= 1:
            return 0

        diff = fabs(cpu_fraction - memory_fraction)
        return int((1 - diff) * float(context.max_priority))

    @staticmethod
    def fraction_of_capacity(requested: int, capacity: int) -> float:
        if capacity == 0:
            capacity = 1
        return float(requested) / float(capacity)


class SelectorSpreadPriority(Priority):
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        # TODO implement spread-scoring
        return 1

    def reduce_mapped_score(self, context: ClusterContext, pod: Pod, nodes: [Node], node_scores: [int]) -> [int]:
        max_count_by_node_name = max(node_scores, default=0)
        result = list(map(lambda node_count: int(context.max_priority * (max_count_by_node_name - node_count) /
                                                 max_count_by_node_name), node_scores))
        return result
