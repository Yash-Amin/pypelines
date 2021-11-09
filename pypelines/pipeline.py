"""Pipeline"""
from yaml import safe_load
from typing import Dict, Any

from pypelines.pipeline_options import PipelineOptions


class Pipeline:
    """Pipeline class to load and run pipeline."""

    def __init__(self) -> None:
        """Init."""
        self._pipeline_yaml = {}
        self.parameters = {}
        self.variables = {}

        self.options: PipelineOptions = None

    def load_from_yaml(
        self,
        pipeline_path: str,
        parameters: Dict[str, Any] = {},
    ) -> None:
        """Load pipeline from yaml file."""
        with open(pipeline_path, "r") as f:
            self._pipeline_yaml = safe_load(f.read())
            self.load(self._pipeline_yaml, parameters)

    def load(
        self,
        pipeline_yaml: Dict[str, Any],
        parameters: Dict[str, Any] = {},
    ) -> None:
        """Create pipeline from pipeline options."""
        # TODO: validate parameters

        # Load pipeline options
        config = pipeline_yaml["config"]
        self.options = PipelineOptions(config, parameters)

    def validate(self) -> bool:
        """Validate pipeline."""
        return True

    def run(self) -> None:
        """Run pipeline."""
        pass
