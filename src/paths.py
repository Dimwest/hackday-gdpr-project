import os
from typing import List


def split_path(path: str) -> List[str]:

    """
    Helper function splitting input path string into a list

    :param path: path string to split
    :return: list containing all path parts
    """

    path_parts = []
    while 1:
        parts = os.path.split(path)
        # sentinel for absolute paths
        if parts[0] == path:
            path_parts.insert(0, parts[0])
            break
        # sentinel for relative paths
        elif parts[1] == path:
            path_parts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            path_parts.insert(0, parts[1])
    return path_parts
