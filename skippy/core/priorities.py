"""
Implementations of the kubernetes default scheduler's priority functions as well as new priority functions (Skippy).
https://github.com/kubernetes/kubernetes/tree/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/scheduler/algorithm/priorities
"""
import logging
from math import fabs
from typing import Dict

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
        logging.debug(f'ResourcePriority: Calculating score for {pod.name} on {node.name}')
        allocatable = node.allocatable
        requested = Capacity()
        requested.memory = 0
        requested.cpu_millis = 0
        for container in pod.spec.containers:
            requested.cpu_millis += container.resources.requests.get("cpu", container.resources.
                                                                     default_milli_cpu_request)
            requested.memory += container.resources.requests.get("memory", container.resources.default_mem_request)

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
        result = int((1 - diff) * float(context.max_priority))
        """
        logging.debug('BalancedResourcePriority: ')
        logging.debug(f'- requested cpu millis: {requested.cpu_millis}')
        logging.debug(f'- allocatable cpu millis: {allocatable.cpu_millis}')
        logging.debug(f'- cpu fraction: {cpu_fraction}')
        logging.debug(f'- requested memory: {requested.memory}')
        logging.debug(f'- allocatable memory: {allocatable.memory}')
        logging.debug(f'- memory fraction: {memory_fraction}')
        logging.debug(f'- diff: {diff}')
        logging.debug(f'- result: {result}')
        """
        return result

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
        # TODO maybe we should handle capabilities like resources (where each deployment decreases the available amount)
        priority = 0
        pod_caps = dict(filter(lambda label: 'capability.skippy.io' in label[0], node.labels.items()))
        # Add 1 for each capability the pod has and the node fulfills (node affinity based on labels)
        for capability in pod_caps.items():
            if capability[0] in pod.spec.labels and capability[1] == pod.spec.labels[capability[0]]:
                priority += 1
        return priority

    def reduce_mapped_score(self, context: ClusterContext, pod: Pod, nodes: [Node], node_scores: [int]) -> [int]:
        # Scale the priorities from 0 to max_priority
        max_count_by_node_name = max(node_scores, default=0)
        if max_count_by_node_name == 0:
            return [0] * len(node_scores)

        result = list(map(lambda node_count: int(context.max_priority * node_count / max_count_by_node_name),
                          node_scores))
        return result


class LocalityPriority(Priority):
    def map_node_score(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        size = self.get_size(context, pod, node)
        target_node = self.get_target_node(context, pod, node)
        bandwidth = context.get_dl_bandwidth(node.name, target_node)
        time = int(size / bandwidth)
        return time

    def reduce_mapped_score(self, context: ClusterContext, pod: Pod, nodes: [Node], node_scores: [int]) -> [int]:
        # Scale the priorities from 0 to max_priority, the lower the node score (time) the higher the priority
        # We do not adjust the values based on the minimum values.
        # Therefore f.e. if there's a download time of 10 and one of 12s, the 12s wouldn't get scored 0 but 8
        min_count_by_node_name = min(node_scores, default=0)
        max_count_by_node_name = max(node_scores, default=0)
        if max_count_by_node_name == 0:
            return [0] * len(node_scores)
        result = list(map(lambda node_count: int(context.max_priority *
                                                 (max_count_by_node_name - node_count + min_count_by_node_name) /
                                                 max_count_by_node_name),
                          node_scores))
        '''
        # Alternative:
        # Adjust to the min value, then score the download times (lowest = 10, highest = 0)
        max_count_by_node_name = max(node_scores, default=0)
        min_count_by_node_name = min(node_scores, default=0)
        if max_count_by_node_name == 0 or max_count_by_node_name == min_count_by_node_name:
            return [0] * len(node_scores)

        result = list(map(lambda node_count: int(context.max_priority *
                                                 (max_count_by_node_name - node_count) /
                                                 (max_count_by_node_name - min_count_by_node_name)), node_scores))
        '''
        return result

    def get_target_node(self, context: ClusterContext, pod: Pod, node: Node) -> str:
        raise NotImplemented()

    def get_size(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        raise NotImplemented()


class LatencyAwareImageLocalityPriority(LocalityPriority):
    def get_size(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        size = 0
        for container in pod.spec.containers:
            image_name = normalize_image_name(container.image)
            if image_name not in context.images_on_nodes[node.name]:
                size += context.get_image_state(image_name).size[node.labels['beta.kubernetes.io/arch']]
        return size

    def get_target_node(self, context: ClusterContext, pod: Pod, node: Node) -> str:
        return 'registry'


class DataLocalityPriority(LocalityPriority):
    def get_size(self, context: ClusterContext, pod: Pod, node: Node) -> int:
        size_from = pod.spec.labels.get('data.skippy.io/receives-from-storage')
        size_to = pod.spec.labels.get('data.skippy.io/sends-to-storage')

        size = 0
        if size_from:
            size += parse_size_string(size_from)
        if size_to:
            size += parse_size_string(size_to)

        return size

    def get_target_node(self, context: ClusterContext, pod: Pod, node: Node) -> str:
        return context.get_next_storage_node(node)
