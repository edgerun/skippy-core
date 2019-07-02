from typing import Dict, List, NamedTuple


class SchedulingResult(NamedTuple):
    suggested_host: str
    evaluated_nodes: int
    feasible_nodes: int


class ImageState:
    size: Dict[str, int]
    num_nodes: int = 0

    def __init__(self, size: Dict[str, int], num_nodes: int = 0):
        self.size = size
        self.num_nodes = num_nodes


class ResourceRequirements:
    """
    API Spec: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#resourcerequirements-v1-core
    Example: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/

    Default values for CPU and MEM requests:
    https://github.com/kubernetes/kubernetes/blob/7f23a743e8c23ac6489340bbb34fa6f1d392db9d/pkg/scheduler/algorithm/priorities/util/non_zero.go#L31

    TODO Handling if something either limit or request is set:
    https://kubernetes.io/docs/tasks/administer-cluster/manage-resources/memory-default-namespace/
    """
    default_milli_cpu_request = 100          # 0,1 cores
    default_mem_request = 200 * 1024 * 1024  # 200 MB

    requests: Dict[str, float] = {"cpu": default_milli_cpu_request, "mem": default_mem_request}


class Container:
    """https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#container-v1-core"""
    resources: ResourceRequirements = ResourceRequirements()
    image: str

    def __init__(self, image: str, resources: ResourceRequirements = None) -> None:
        super().__init__()
        self.resources = resources or ResourceRequirements()
        self.image = image


class PodSpec:
    """https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#podspec-v1-core"""
    containers: List[Container]

    def __init__(self, containers: List[Container] = None) -> None:
        super().__init__()
        if containers is None:
            containers = []
        self.containers = containers


class Pod:
    """
    A Pod represents a running process on your cluster.
    """
    name: str
    spec: PodSpec

    def __init__(self, name: str, spec: PodSpec = None) -> None:
        super().__init__()
        self.name = name
        self.spec = spec


class Capacity:
    """
    Node capacity
    """

    def __init__(self, cpu_millis: int = 1 * 1000, memory: int = 1024 * 1024 * 1024):
        self.memory = memory
        self.cpu_millis = cpu_millis

    def __str__(self):
        return 'Capacity(CPU: {0} Memory: {1})'.format(self.cpu_millis, self.memory)


class Node:
    """
    A node is a worker machine in Kubernetes to run pods.
    """
    name: str
    pods: List[Pod]
    capacity: Capacity
    allocatable: Capacity  # This contains the remaining allocatable capacity
    labels: Dict[str, str]

    def __init__(self, name: str, capacity: Capacity = None, allocatable: Capacity = None,
                 labels: Dict[str, str] = None) -> None:
        super().__init__()
        self.name = name
        self.capacity = capacity or Capacity()
        self.allocatable = allocatable or Capacity()
        self.labels = labels or {}
        self.pods = list()
