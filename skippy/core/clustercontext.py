from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Dict

from core.model import Node, Pod, ImageState
from core.utils import normalize_image_name


class ClusterContext(ABC):
    # Dict holding image metadata for each (normalized) image tag
    image_states: Dict[str, ImageState] = {}

    # Defines the maximum score of a single node
    max_priority: int = 10

    # Dict to maintain which node has which images in the local registry
    images_on_nodes: Dict[str, Dict[str, ImageState]] = defaultdict(dict)

    # Dict to maintain the bandwidth graph
    # bandwidth[from][to] = bandwidth in bytes per second
    bandwidth: Dict[str, Dict[str, float]] = defaultdict(dict)

    def __init__(self):
        # https://cloud.docker.com/v2/repositories/alexrashed/ml-wf-1-pre/tags/0.33/
        # https://cloud.docker.com/v2/repositories/alexrashed/ml-wf-2-train/tags/0.33/
        # https://cloud.docker.com/v2/repositories/alexrashed/ml-wf-3-serve/tags/0.33/
        self.image_states = {
            'alexrashed/ml-wf-1-pre:0.33': ImageState(size={
                'arm': 461473086,
                'arm64': 538015840,
                'amd64': 530300745
            }),
            'alexrashed/ml-wf-2-train:0.33': ImageState(size={
                'arm': 506029298,
                'arm64': 582828211,
                'amd64': 547365470
            }),
            'alexrashed/ml-wf-3-serve:0.33': ImageState(size={
                'arm': 506769993,
                'arm64': 585625232,
                'amd64': 585928717
            })
        }

        # TODO move to KubeClusterContext and implement bandwidth synthesizer in sim package
        # 1.25e+7 Byte/s= 100 MBit/s
        self.bandwidth = {
            'ara-clustercloud1': {
                'ara-clustertegra1': 1.25e+7,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clustertegra1': {
                'ara-clustercloud1': 1.25e+7,
                'ara-clusterpi1': 1.25e+7,
                'ara-clusterpi2': 1.25e+7,
                'ara-clusterpi3': 1.25e+7,
                'ara-clusterpi4': 1.25e+7,
                'registry': 1.25e+7
            },
            'ara-clusterpi1': {
                'ara-clustercloud1':  1.25e+7,
                'ara-clustertegra1':  1.25e+7,
                'ara-clusterpi2':  1.25e+7,
                'ara-clusterpi3':  1.25e+7,
                'ara-clusterpi4':  1.25e+7,
                'registry':  1.25e+7
            },
            'ara-clusterpi2': {
                'ara-clustercloud1':  1.25e+7,
                'ara-clustertegra1':  1.25e+7,
                'ara-clusterpi1':  1.25e+7,
                'ara-clusterpi3':  1.25e+7,
                'ara-clusterpi4':  1.25e+7,
                'registry':  1.25e+7
            },
            'ara-clusterpi3': {
                'ara-clustercloud1':  1.25e+7,
                'ara-clustertegra1':  1.25e+7,
                'ara-clusterpi1':  1.25e+7,
                'ara-clusterpi2':  1.25e+7,
                'ara-clusterpi4':  1.25e+7,
                'registry':  1.25e+7
            },
            'ara-clusterpi4': {
                'ara-clustercloud1':  1.25e+7,
                'ara-clustertegra1':  1.25e+7,
                'ara-clusterpi1':  1.25e+7,
                'ara-clusterpi2':  1.25e+7,
                'ara-clusterpi3':  1.25e+7,
                'registry':  1.25e+7
            }
        }

    @abstractmethod
    def list_nodes(self) -> List[Node]:
        raise NotImplemented()

    @abstractmethod
    def get_next_storage_node(self, node: Node):
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
            node.allocatable.memory -= container.resources.requests.get('mem', container.resources.default_mem_request)
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
        raise NotImplemented("Remote requested size information about images are not yet supported.")

    def get_dl_bandwidth(self, from_node: str, to_node: str) -> float:
        return self.bandwidth[from_node][to_node]
