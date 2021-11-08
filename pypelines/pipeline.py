"""Pipeline"""
from yaml import safe_load
from typing import Dict, Any


class Pipeline:
    """Pipeline class to load and run pipeline."""

    def __init__(self) -> None:
        """Init."""
        self._pipeline_yaml = {}
        self.parameters = {}
        self.variables = {}

    def load_from_yaml(
        self,
        pipeline_path: str,
        parameters: Dict[str, Any] = {},
    ) -> None:
        """Load pipeline from yaml file."""
        with open(pipeline_path, "r") as f:
            pipeline_obj = safe_load(f.read())
            self.load(pipeline_obj, parameters)

    def load(
        self,
        pipeline_options: Dict[str, Any],
        parameters: Dict[str, Any] = {},
    ) -> None:
        """Create pipeline from pipeline options."""
        pass

    def validate(self) -> bool:
        """Validate pipeline."""
        return True

    def run(self) -> None:
        """Run pipeline."""
        pass
