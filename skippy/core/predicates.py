"""
Implementations of the kubernetes default scheduler's predicates.
https://github.com/kubernetes/kubernetes/blob/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/scheduler/algorithm/predicates/
"""
from core.clustercontext import ClusterContext
from core.model import Pod, Node, Capacity


class Predicate:
    """Abstract class for predicate implementations."""
    def passes_predicate(self, context: ClusterContext, pod: Pod, node: Node) -> bool:
        raise NotImplementedError


class CombinedPredicate(Predicate):
    """Helper-Super-Class to combine multiple predicates to a conjunction."""
    def __init__(self, predicates: [Predicate]):
        self.predicates = predicates

    def passes_predicate(self, context: ClusterContext, pod: Pod, node: Node) -> bool:
        return all(predicate.passes_predicate(context, pod, node) for predicate in self.predicates)


class PodFitsResourcesPred(Predicate):
    """
    Part of NonCriticalPreds!
    PodFitsResources checks if a node has sufficient resources, such as cpu, memory, gpu, opaque int resources etc to
    run a pod.
    https://github.com/kubernetes/kubernetes/blob/eaa78b88ac25a61bfb1aa81d118c5ffeda041b64/pkg/scheduler/algorithm/predicates/predicates.go#L769
    """
    def passes_predicate(self, context: ClusterContext, pod: Pod, node: Node) -> bool:
        allocatable = node.allocatable
        requested = Capacity()
        requested.memory = 0
        requested.cpu_millis = 0
        requested.max_pods = 0
        for container in pod.spec.containers:
            requested.cpu_millis += container.resources.requests["cpu"]
            requested.memory += container.resources.requests["mem"]
        return node.allocatable.max_pods > 0 and requested.memory <= allocatable.memory and \
            requested.cpu_millis <= allocatable.cpu_millis


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
