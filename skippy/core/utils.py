import re

# https://github.com/kubernetes/kubernetes/blob/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/util/parsers/parsers.go#L30
import time

default_image_tag: str = "latest"


# https://github.com/kubernetes/kubernetes/blob/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/scheduler/algorithm/priorities/image_locality.go#L104
def normalize_image_name(image_name: str):
    if image_name.rfind(":") <= image_name.rfind("/"):
        image_name = image_name + ":" + default_image_tag
    return image_name


__size_conversions = {
    'K': 10 ** 3,
    'M': 10 ** 6,
    'G': 10 ** 9,
    'T': 10 ** 12,
    'P': 10 ** 15,
    'E': 10 ** 18,
    'Ki': 2 ** 10,
    'Mi': 2 ** 20,
    'Gi': 2 ** 30,
    'Ti': 2 ** 40,
    'Pi': 2 ** 50,
    'Ei': 2 ** 60
}

__size_pattern = re.compile(r"([0-9]+)([a-zA-Z]*)")


def parse_size_string(size_string: str) -> int:
    m = __size_pattern.match(size_string)
    if len(m.groups()) > 1:
        number = m.group(1)
        unit = m.group(2)
        return int(number) * __size_conversions.get(unit, 1)
    else:
        return int(m.group(1))


class Timer:

    def __init__(self) -> None:
        super().__init__()
        self.then = -1

    def start(self):
        self.then = time.time()
        return self

    def ms(self):
        return (time.time() - self.then) * 1000


def counter(start: int = 1):
    n = start
    while True:
        yield n
        n += 1
