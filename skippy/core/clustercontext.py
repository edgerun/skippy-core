from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Dict

from core.model import Node, Pod, ImageState
from core.utils import normalize_image_name

BandwidthGraph = Dict[str, Dict[str, float]]


class ClusterContext(ABC):
    # Dict holding image metadata for each (normalized) image tag
    image_states: Dict[str, ImageState] = {}

    # Defines the maximum score of a single node
    max_priority: int = 10

    # Dict to maintain which node has which images in the local registry
    images_on_nodes: Dict[str, Dict[str, ImageState]] = defaultdict(dict)

    # Dict to maintain the bandwidth graph
    # bandwidth[from][to] = bandwidth in bytes per second
    bandwidth: BandwidthGraph = defaultdict(dict)

    def __init__(self):
        self.image_states = self.get_init_image_states()
        self.bandwidth = self.get_bandwidth_graph()

    @abstractmethod
    def get_init_image_states(self) -> Dict[str, ImageState]:
        raise NotImplemented()

    @abstractmethod
    def get_bandwidth_graph(self) -> Dict[str, Dict[str, float]]:
        raise NotImplemented()

    @abstractmethod
    def list_nodes(self) -> List[Node]:
        raise NotImplemented()

    @abstractmethod
    def get_next_storage_node(self, node: Node) -> str:
        raise NotImplemented()

    def place_pod_on_node(self, pod: Pod, node: Node):
        """
        Method to keep track of already placed pods on nodes in order to allow calculating the remaining resources on a
        node.
        """
        for container in pod.spec.containers:
            image_name = normalize_image_name(container.image)
            image_state = self.get_image_state(image_name)

            image_state.num_nodes += 1

            images_on_nodes = self.images_on_nodes[node.name]
            images_on_nodes[container.image] = image_state
            self.images_on_nodes[node.name][container.image] = image_state

            node.allocatable.cpu_millis -= container.resources.requests.get('cpu', container.resources.
                                                                            default_milli_cpu_request)
            node.allocatable.memory -= container.resources.requests.get('memory', container.resources.default_mem_request)
        node.pods.append(pod)

    def get_image_state(self, image_name: str) -> ImageState:
        """
        Finds metadata about the image.
        :returns ImageState
        """
        if self.image_states[image_name] is None:
            self.image_states[image_name] = self.retrieve_image_state(image_name)
        return self.image_states[image_name]

    def retrieve_image_state(self, image_name):
        # TODO maybe implement docker integration? There's no proper documented API, but f.e.
        #  https://cloud.docker.com/v2/repositories/alexrashed/ml-wf-1-pre/tags/0.33/
        #  returns a JSON containing the size
        raise NotImplemented("Remote requested size information about images are not yet supported.")

    def get_dl_bandwidth(self, from_node: str, to_node: str) -> float:
        return self.bandwidth[from_node][to_node]
