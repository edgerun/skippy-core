from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Dict

from core.model import Node, Pod, ImageState
from core.utils import normalize_image_name


class ClusterContext(ABC):

    # Dict holding image metadata
    image_states: Dict[str, ImageState] = {}

    # Defines the maximum score of a single node
    max_priority: int = 10

    # Dict to maintain which node has which images in the local registry
    images_on_nodes: Dict[str, Dict[str, ImageState]] = defaultdict(dict)

    @abstractmethod
    def list_nodes(self) -> List[Node]:
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

            node.allocatable.cpu_millis -= container.resources.requests["cpu"]
            node.allocatable.memory -= container.resources.requests["mem"]

        node.allocatable.max_pods -= 1

    def get_image_state(self, image_name: str) -> ImageState:
        """
        Finds metadata about the image.
        :returns ImageState
        """
        if self.image_states[image_name] is None:
            self.image_states[image_name] = self.retrieve_image_state(image_name)
        return self.image_states[image_name]

    @abstractmethod
    def retrieve_image_state(self, image_name):
        raise NotImplemented()
