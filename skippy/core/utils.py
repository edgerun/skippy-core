# https://github.com/kubernetes/kubernetes/blob/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/util/parsers/parsers.go#L30
default_image_tag: str = "latest"


# https://github.com/kubernetes/kubernetes/blob/e318642946daab9e0330757a3556a1913bb3fc5c/pkg/scheduler/algorithm/priorities/image_locality.go#L104
def normalize_image_name(image_name: str):
    if image_name.rfind(":") <= image_name.rfind("/"):
        image_name = image_name + ":" + default_image_tag
    return image_name
