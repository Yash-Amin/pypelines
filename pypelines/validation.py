"""Functions for validation."""
import re


def output_parameter_name(s: str) -> str:
    """Validates output parameter name."""
    s = str(s)

    if not s:
        raise ValueError("Output parameter name is empty.")

    if not re.match("^[a-zA-Z0-9_\-]+$", s):
        raise ValueError(f"Output parameter name '{s}' contains invalid characters.")

    return s
