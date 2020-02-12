from typing import Dict, List, NamedTuple


class ImageState:
    size: Dict[str, int]
    num_nodes: int = 0

    def __init__(self, size: Dict[str, int], num_nodes: int = 0):
        self.size = size
        self.num_nodes = num_nodes

    def __str__(self) -> str:
        return "ImageState%s" % self.__dict__

    def __repr__(self):
        return self.__str__()

class ResourceRequirements:
    """
    API Spec: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#resourcerequirements-v1-core
    Example: https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/

    Default values for CPU and MEM requests:
    https://github.com/kubernetes/kubernetes/blob/7f23a743e8c23ac6489340bbb34fa6f1d392db9d/pkg/scheduler/algorithm/priorities/util/non_zero.go#L31

    TODO Handling if something either limit or request is set:
    https://kubernetes.io/docs/tasks/administer-cluster/manage-resources/memory-default-namespace/
    """
    default_milli_cpu_request = 100  # 0,1 cores
    default_mem_request = 200 * 1024 * 1024  # 200 MB

    default_requests: Dict[str, float] = {"cpu": default_milli_cpu_request, "memory": default_mem_request}

    def __init__(self, requests: Dict[str, float] = None) -> None:
        super().__init__()
        self.requests = requests or dict(ResourceRequirements.default_requests)


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
    labels: Dict[str, str]

    def __init__(self, containers: List[Container] = None, labels: Dict[str, str] = None) -> None:
        super().__init__()
        if containers is None:
            containers = []
        if labels is None:
            labels = {}
        self.containers = containers
        self.labels = labels


class Pod:
    """
    A Pod represents a running process on your cluster.
    """
    name: str
    namespace: str
    spec: PodSpec

    def __init__(self, name: str, namespace: str, spec: PodSpec = None) -> None:
        super().__init__()
        self.name = name
        self.namespace = namespace
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
    allocatable: Capacity  # This variable is stateful and contains the *remaining* allocatable capacity
    labels: Dict[str, str]

    def __init__(self, name: str, capacity: Capacity = None, allocatable: Capacity = None,
                 labels: Dict[str, str] = None) -> None:
        super().__init__()
        self.name = name
        self.capacity = capacity or Capacity()
        self.allocatable = allocatable or Capacity()
        self.labels = labels or {}
        self.pods = list()

    def __repr__(self):
        return self.name


class SchedulingResult(NamedTuple):
    suggested_host: Node
    feasible_nodes: int
    needed_images: List[str]  # Defines which images need to be pulled on the selected node
