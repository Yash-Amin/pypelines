"""Pipeline"""
from os import name, pipe
from yaml import safe_load
from typing import Dict, Any, List
from dataclasses import dataclass

from pypelines import utils
from pypelines.tasks import tasks
from pypelines.task import PipelineTask
from pypelines.pipeline_options import PipelineOptions
from pypelines.utils import replace_parameters_from_anything


@dataclass
class PipelineParameter:
    """Pipeline parameter"""

    name: str = None
    value: Any = None
    allowed_values: List[Any] = None
    default: Any = None
    description: str = None

    def __post_init__(self):
        """Validate parameter"""
        if self.default is not None and self.value is None:
            self.value = self.default

        if self.allowed_values is not None:
            if self.value not in self.allowed_values:
                raise ValueError(
                    "Parameter '{}' must be one of {}, provided '{}'".format(
                        self.name, self.allowed_values, self.value
                    )
                )


class Pipeline:
    """Pipeline class to load and run pipeline."""

    def __init__(self) -> None:
        """Init."""
        self._pipeline_yaml = {}
        self.parameters: List[PipelineParameter] = []
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
        parameter_values: Dict[str, Any] = {},
    ) -> None:
        """Create pipeline from pipeline options."""
        self._pipeline_yaml = pipeline_yaml

        self.load_parameters(parameter_values)

        # Load pipeline options
        config = pipeline_yaml["config"]
        self.options = PipelineOptions()
        self.options.load(config, self.get_parameter_values())

        self.validate()

    def load_parameters(self, parameter_values: Dict[str, Any]) -> None:
        """Parses and validates parameters.

        Arguments:
            parameter_values {Dict[str, Any]} -- Parameter values dictioanry
                    where key is parameter name and value is parameter value.
        """
        # """Converts list of parameter schema to dictionary"""
        parameters_schema: Dict[str, Any] = {}
        for parameter_schema in self._pipeline_yaml.get("parameters", []):
            if parameter_schema["name"] in parameters_schema:
                raise ValueError(
                    "Multiple parameters with same name '{}' provided".format(
                        parameter_schema["name"]
                    )
                )

            parameters_schema[parameter_schema["name"]] = parameter_schema

        # Sets values for parameters
        for param_name, parameter_schema in parameters_schema.items():
            _param_raw_value = (
                parameter_schema.get("default")
                if parameter_values.get(param_name) is None
                else parameter_values.get(param_name)
            )

            param_value = replace_parameters_from_anything(
                _param_raw_value, self.get_parameter_values()
            )

            if param_value is None:
                raise ValueError("Parameter '{}' is required".format(param_name))

            param = PipelineParameter(
                name=param_name,
                default=parameter_schema.get("default"),
                allowed_values=parameter_schema.get("allowed-values"),
                description=parameter_schema.get("description"),
                value=param_value,
            )

            self.parameters.append(param)

        # Raise error if required parameter having no default value specified
        # in the pipeline schema is not provided
        for param_name in parameter_values:
            if param_name not in parameters_schema:
                raise ValueError(
                    "Parameter '{}' is not defined in the pipeline schema".format(
                        param_name
                    )
                )

    def validate(self) -> None:
        """Validate pipeline."""
        pass

    def get_parameter_values(self) -> Dict[str, Any]:
        """Returns dict of parameter values."""
        return {parameter.name: parameter.value for parameter in self.parameters}

    def run(self) -> None:
        """Run pipeline."""
        print("Running pipeline")

        for task_config in self._pipeline_yaml["tasks"]:
            task_type = task_config["task"]
            if task_type not in tasks:
                raise ValueError("Task of '{}' type was not found.".format(task_type))

            task_name = utils.replace_parameters_from_string(
                task_config["name"], self.options.parameters
            )

            task_input_values = task_config.get("inputs", {})

            task: PipelineTask = tasks[task_type](
                name=task_name,
                task_input_values=task_input_values,
                pipeline_parameters=self.options.parameters.copy(),
                pipeline_options=self.options,
            )

            task.run()
