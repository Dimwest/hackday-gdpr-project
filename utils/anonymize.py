from hashlib import sha256
import random


def anonymize_value(val, encoding):

    """
    Generate an unusable value of the same type as the input value
    Works with JSON-compliant types
    Works recursively for sequence and dict types
    Raises TypeError if type is not a JSON-compliant type

    :param val: value to anonymize
    :param encoding: configured encoding to handle text data

    :return: anonymized value
    """

    # If value is string, hash it
    if isinstance(val, str):
        return sha256(val.encode(encoding)).hexdigest()

    # If value is list, anonymize all of its elements
    elif isinstance(val, list):
        return [anonymize_value(x, encoding) for x in val]

    # If value is float, generate random float
    elif isinstance(val, float):
        secure_random = random.SystemRandom()
        return secure_random.random()

    # If value is int, generate random 0 or 1
    elif isinstance(val, int):
        secure_random = random.SystemRandom()
        return secure_random.randint(0, 1)

    # If value is bool, pick random boolean
    elif isinstance(val, bool):
        return random.random() >= 0.5

    # If value is dict, anonymize all of its values
    elif isinstance(val, dict):
        return {k: anonymize_value(k) for k in val.keys()}

    elif val is None:
        return None

    else:
        raise TypeError("Incorrect object type: must be one of: str, int, float, list, bool, dict, Nonetype")


def anonymize_nested_value(dic, keys, encoding):

    """
    Anonymize specified dictionary key

    :param dic: target dictionary
    :param keys: list of keys from the top level to the target, e.g. for
    meta.city.name it should be ['meta', 'city', 'name']

    :param encoding: configured encoding to handle text data
    """

    for key in keys[:-1]:
        dic = dic.setdefault(key, {})

    if dic.get(keys[-1]):
        dic[keys[-1]] = anonymize_value(dic[keys[-1]], encoding)


