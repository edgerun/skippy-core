"""
Implementations of the kubernetes default scheduler's predicates.
https://github.com/kubernetes/kubernetes/blob/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/scheduler/algorithm/predicates/
"""
import logging

from core.clustercontext import ClusterContext
from core.model import Pod, Node, Capacity

logger = logging.getLogger(__name__)


class Predicate:
    """Abstract class for predicate implementations."""

    def passes_predicate(self, context: ClusterContext, pod: Pod, node: Node) -> bool:
        raise NotImplementedError


class CombinedPredicate(Predicate):
    """Helper-Super-Class to combine multiple predicates to a conjunction."""

    def __init__(self, predicates: [Predicate]):
        self.predicates = predicates

    def passes_predicate(self, context: ClusterContext, pod: Pod, node: Node) -> bool:
        return all(self.__passes_and_logs_predicate(predicate, context, pod, node)
                   for predicate in self.predicates)

    # noinspection PyMethodMayBeStatic
    def __passes_and_logs_predicate(self, predicate: Predicate, context: ClusterContext, pod: Pod, node: Node):
        result = predicate.passes_predicate(context, pod, node)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Pod {pod.name} / Node {node.name} / {type(predicate).__name__}: '
                         f'{"Passed" if result else "Failed"}')

        return result


class PodFitsResourcesPred(Predicate):
    """
    Part of NonCriticalPreds!
    PodFitsResources checks if a node has sufficient resources, such as cpu, memory, gpu, opaque int resources etc to
    run a pod.
    https://github.com/kubernetes/kubernetes/blob/eaa78b88ac25a61bfb1aa81d118c5ffeda041b64/pkg/scheduler/algorithm/predicates/predicates.go#L769
    """

    def passes_predicate(self, context: ClusterContext, pod: Pod, node: Node) -> bool:
        allocatable = node.allocatable
        requested = Capacity(0, 0)
        for container in pod.spec.containers:
            requested.cpu_millis += container.resources.requests.get('cpu', container.resources.
                                                                     default_milli_cpu_request)
            requested.memory += container.resources.requests.get('memory', container.resources.default_mem_request)
        passed = requested.memory <= allocatable.memory and requested.cpu_millis <= allocatable.cpu_millis

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Pod {pod.name} requests {requested.cpu_millis} / {requested.memory}. '
                         f'Available on node {node.name}: {allocatable.cpu_millis} / {allocatable.memory}.'
                         f'Passed: {passed}')
        return passed


class NonCriticalPreds(CombinedPredicate):
    """
    Part of GeneralPreds!
    NoncriticalPredicates are the predicates that only non-critical pods need
    https://github.com/kubernetes/kubernetes/blob/eaa78b88ac25a61bfb1aa81d118c5ffeda041b64/pkg/scheduler/algorithm/predicates/predicates.go#L1134
    """

    def __init__(self):
        super().__init__([PodFitsResourcesPred()])


class EssentialPreds(CombinedPredicate):
    """
    Part of GeneralPreds!
    EssentialPredicates are the predicates that all pods, including critical pods, need
    https://github.com/kubernetes/kubernetes/blob/eaa78b88ac25a61bfb1aa81d118c5ffeda041b64/pkg/scheduler/algorithm/predicates/predicates.go#L1148
    """

    def __init__(self):
        super().__init__([PodFitsResourcesPred()])


class GeneralPreds(CombinedPredicate):
    """
    GeneralPredicates checks whether noncriticalPredicates and EssentialPredicates pass.
    noncriticalPredicates are the predicates that only non-critical pods need and EssentialPredicates are the predicates
    that all pods, including critical pods, need
    https://github.com/kubernetes/kubernetes/blob/eaa78b88ac25a61bfb1aa81d118c5ffeda041b64/pkg/scheduler/algorithm/predicates/predicates.go#L1110
    """

    def __init__(self):
        # NonCriticalPreds should only be applied if the Pod is non-critical,
        # but we don't handle critical pods in our simulation
        super().__init__([EssentialPreds(), NonCriticalPreds()])
