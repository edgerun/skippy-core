from collections import defaultdict
from typing import Dict, Set, NamedTuple, Tuple


class DataItem(NamedTuple):
    """
    A DataItem refers to an item in an object store, for example an S3 (simple storage service) object.
    """
    bucket: str
    name: str
    size: int


class StorageIndex:
    """
    The StorageIndex keeps track of data items on a set of storage nodes in the cluster.
    Currently this is only a dummy in-memory implementation.
    """
    buckets: Dict[str, Set[str]]
    tree: Dict[Tuple[str, str], Set[str]]
    items: Dict[Tuple[str, str], DataItem]

    def __init__(self) -> None:
        super().__init__()
        self.buckets = defaultdict(set)
        self.tree = defaultdict(set)
        self.items = dict()

    def mb(self, name: str, node: str):
        """
        Create a bucket on a node.

        :param name: the bucket name
        :param node: the node to create the bucket on
        """
        self.buckets[name].add(node)

    def put(self, data: DataItem):
        nodes = self.get_bucket_nodes(data.bucket)
        if not nodes:
            raise KeyError('no nodes that host bucket %s' % data.bucket)

        k = (data.bucket, data.name)
        self.items[k] = data

        for node in nodes:
            self.tree[k].add(node)

    def stat(self, bucket: str, name: str) -> DataItem:
        k = (bucket, name)
        return self.items.get(k)

    def get_bucket_nodes(self, bucket: str) -> Set[str]:
        return self.buckets[bucket]

    def get_data_nodes(self, bucket: str, name: str) -> Set[str]:
        k = (bucket, name)
        return self.tree.get(k)

    def print_ls_tree(self):
        """
        Debug function to print the entire tree in a `find`-style output.
        """
        tree = defaultdict(lambda: defaultdict(list))
        for (bucket, item), nodes in self.tree.items():
            for node in nodes:
                tree[node][bucket].append(item)

        for node, buckets in tree.items():
            print(f'/{node}')

            for bucket, items in buckets.items():
                print(f'/{node}/{bucket}')

                for item in items:
                    print(f'/{node}/{bucket}/{item}')
