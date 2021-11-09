"""Util functions."""


from typing import Any, Dict, List


def string_to_bool(val):
    """Convert string to bool."""
    if val is None:
        return False

    if isinstance(val, bool):
        return val

    if isinstance(val, str):
        if val.lower() in ("yes", "true", "t", "y", "1"):
            return True
        elif val.lower() in ("no", "false", "f", "n", "0"):
            return False

    raise ValueError("Boolean value expected.")


def replace_parameters_from_string(s: str, parameters: Dict[str, Any]) -> str:
    """Replace parameters in string."""
    s = str(s)

    for key, value in parameters.items():
        s = s.replace("${{parameters." + str(key) + "}}", str(value))

    return s


def replace_parameters_from_list(
    array: List[Any], parameters: Dict[str, Any]
) -> List[Any]:
    """Replaces parameters from list items."""
    return [replace_parameters_from_anything(item, parameters) for item in array]


def replace_parameters_from_dict(
    obj: Dict[str, Any], parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """Replaces parameters from keys and values of dict."""
    return {
        replace_parameters_from_string(
            key, parameters
        ): replace_parameters_from_anything(value, parameters)
        for key, value in obj.items()
    }


def replace_parameters_from_anything(obj: Any, parameters: Dict[str, Any]) -> Any:
    """Replace parameters from if type of obj is dict,list,str, else returns obj."""
    if isinstance(obj, list):
        return replace_parameters_from_list(obj, parameters)
    elif isinstance(obj, dict):
        return replace_parameters_from_dict(obj, parameters)
    elif isinstance(obj, str):
        return replace_parameters_from_string(obj, parameters)

    # For all other type
    return obj
