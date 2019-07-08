"""
Implementations of the kubernetes default scheduler's priority functions.
https://github.com/kubernetes/kubernetes/tree/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/scheduler/algorithm/priorities
"""
from typing import Dict

from math import fabs

from core.clustercontext import ClusterContext
from core.model import Pod, Node, Capacity, ImageState
from core.utils import normalize_image_name, parse_size_string


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
                    calc_sum += self.scaled_image_score(node, image_state, total_num_nodes)
                except KeyError:
                    pass
        return calc_sum

    # noinspection PyMethodMayBeStatic
    def scaled_image_score(self, node: Node, image_state: ImageState, total_num_nodes: int) -> int:
        spread = float(image_state.num_nodes) / float(total_num_nodes)
        return int(float(image_state.size[node.labels['beta.kubernetes.io/arch']]) * spread)


class ResourcePriority(Priority):
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        allocatable = node.allocatable
        requested = Capacity()
        requested.memory = 0
        requested.cpu_millis = 0
        requested.max_pods = 0
        for container in pod.spec.containers:
            requested.cpu_millis += container.resources.requests.get("cpu", container.resources.
                                                                     default_milli_cpu_request)
            requested.memory += container.resources.requests.get("mem", container.resources.default_mem_request)

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


class LocalityTypePriority(Priority):
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        # Either return the priority for the type label or 0
        priority_mapping: Dict[str, int] = {
            # Give edge nodes the highest priority
            'edge': context.max_priority,
            'cloud': 0
        }
        try:
            return priority_mapping.get(node.labels['locality.skippy.io/type'], 0)
        except KeyError:
            return 0


class CapabilityPriority(Priority):
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        # TODO scale priority from 0 to max
        priority = 0
        pod_caps = dict(filter(lambda label: 'capability.skippy.io' in label[0], node.labels.items()))
        # Add 1 for each capability the pod has and the node fulfills (node affinity based on labels)
        for capability in pod_caps.items():
            if capability[0] in node.labels and capability[1] == node.labels[capability[0]]:
                priority += 1
        return priority

    def reduce_mapped_score(self, context: ClusterContext, pod: Pod, nodes: [Node], node_scores: [int]) -> [int]:
        # Scale the priorities from 0 to max_priority
        max_count_by_node_name = max(node_scores, default=0)
        result = list(map(lambda node_count: int(context.max_priority * (max_count_by_node_name - node_count) /
                                                 max_count_by_node_name), node_scores))
        return result


class LocalityPriority(Priority):
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        size = self.get_size(context, pod, node)
        target_node = self.get_target_node(context, pod, node)
        bandwidth = context.get_dl_bandwidth(node.name, target_node)
        time = size / bandwidth
        # TODO
        # Time (s) = Size (KB) / Bandwidth from node to registry (KB/s)
        # Rate the resulting time -> Lower is better
        # Scale the resulting score from 0 to max
        score = int(1000 / time)
        return score

    def reduce_mapped_score(self, context: ClusterContext, pod: Pod, nodes: [Node], node_scores: [int]) -> [int]:
        # Scale the priorities from 0 to max_priority
        max_count_by_node_name = max(node_scores, default=0)
        result = list(map(lambda node_count: int(context.max_priority * (max_count_by_node_name - node_count) /
                                                 max_count_by_node_name), node_scores))
        return result

    def get_target_node(self, context: ClusterContext, pod: Pod, node: Node) -> str:
        raise NotImplemented()

    def get_size(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        raise NotImplemented()


class LatencyAwareImageLocalityPriority(LocalityPriority):
    def get_size(self, context: ClusterContext, pod: Pod, node: Node):
        size = 0
        for container in pod.spec.containers:
            image_name = normalize_image_name(container.image)
            if image_name not in context.images_on_nodes[node.name]:
                size += context.get_image_state(image_name).size[node.labels['beta.kubernetes.io/arch']]
        raise size

    def get_target_node(self, context: ClusterContext, pod: Pod, node: Node) -> str:
        return 'registry'


class DataLocalityPriority(LocalityPriority):
    def get_size(self, context: ClusterContext, pod: Pod, node: Node):
        size = parse_size_string(pod.spec.labels.get('data.skippy.io/receives-from-storage', '0'))
        size += parse_size_string(pod.spec.labels.get('data.skippy.io/sends-to-storage', '0'))
        return size

    def get_target_node(self, context: ClusterContext, pod: Pod, node: Node):
        return context.get_next_storage_node(node)
