from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Dict

from core.model import Node, Pod, ImageState
from core.utils import normalize_image_name

BandwidthGraph = Dict[str, Dict[str, float]]


class ClusterContext(ABC):

    def __init__(self):
        # Dict holding image metadata for each (normalized) image tag
        self.image_states: Dict[str, ImageState] = self.get_init_image_states()

        # Defines the maximum score of a single node
        self.max_priority: int = 10

        # Dict to maintain which node has which images in the local registry
        self.images_on_nodes: Dict[str, Dict[str, ImageState]] = defaultdict(dict)

        # Dict to maintain the bandwidth graph
        # bandwidth[from][to] = bandwidth in bytes per second
        self.bandwidth: BandwidthGraph = self.get_bandwidth_graph()

    def get_node(self, name: str) -> Node:
        for node in self.list_nodes():
            if node.name == name:
                return node

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

            if image_name not in self.images_on_nodes[node.name]:
                image_state = self.get_image_state(image_name)

                image_state.num_nodes += 1

                images_on_nodes = self.images_on_nodes[node.name]
                images_on_nodes[image_name] = image_state
                self.images_on_nodes[node.name][image_name] = image_state  # FIXME: isn't this the same statement?

            required_cpu_millis = container.resources.requests.get('cpu', container.resources.default_milli_cpu_request)
            required_memory = container.resources.requests.get('memory', container.resources.default_mem_request)

            node.allocatable.cpu_millis -= required_cpu_millis
            node.allocatable.memory -= required_memory
        node.pods.append(pod)

    def remove_pod_from_node(self, pod: Pod, node: Node):
        for container in pod.spec.containers:
            required_cpu_millis = container.resources.requests.get('cpu', container.resources.default_milli_cpu_request)
            required_memory = container.resources.requests.get('memory', container.resources.default_mem_request)

            node.allocatable.cpu_millis += required_cpu_millis
            node.allocatable.memory += required_memory
        node.pods.remove(pod)

    def remove_pod_images_from_node(self, pod: Pod, node: Node):
        for container in pod.spec.containers:
            image_name = normalize_image_name(container.image)

            if image_name in self.images_on_nodes[node.name]:
                image_state = self.get_image_state(image_name)
                image_state.num_nodes -= 1
                del self.images_on_nodes[node.name][image_name]

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

    def get_image_sizes(self, pod: Pod, arch='amd64') -> Dict[str, int]:
        """
        Returns a dictionary with the image sizes
        :param pod:
        :param arch:
        :return:
        """
        return {container.image: self.get_image_state(container.image).size[arch] for container in pod.spec.containers}
