"""Abstract class for PipelineTask"""
from dataclasses import dataclass
from typing import Any, Dict, List

from pypelines import utils
from pypelines.pipeline_options import PipelineOptions


@dataclass
class TaskInputSchema:
    """Schema for task input"""

    name: str
    default_value: str = None
    allow_parameters: bool = True
    allowed_values: List[str] = None
    description: str = None
    # if required is true and value is None then validation will fail
    # but if required is false it will allow None value
    required: bool = True


class PipelineTask:
    """Base class for pipeline task"""

    # Task type
    task_type: str = "Task"

    # Task name
    name: str = None

    # Task input schema
    task_input_schema: List[TaskInputSchema] = []

    def __init__(
        self,
        name: str,
        task_input_values: Dict[str, Any],
        pipeline_parameters: Dict[str, Any],
        pipeline_options: PipelineOptions,
    ) -> None:
        self.name = name
        self.pipeline_options: PipelineOptions = pipeline_options
        self.task_input_values: Dict[str, Any] = task_input_values
        self.pipeline_parameters: Dict[str, Any] = pipeline_parameters

    def get_parsed_inputs(self) -> Dict[str, Any]:
        """Return parsed task input values."""
        unique_input_keys = set([x.name for x in self.task_input_schema])

        # If invalid key is provided, then raise error
        for key in self.task_input_values:
            if key not in unique_input_keys:
                raise ValueError(f"{key} is not a valid task input")

        # Store input values in a dictionary
        parsed_input_values: Dict[str, Any] = {}

        # Set default values for each input
        for task_input in self.task_input_schema:
            parsed_input_values[task_input.name] = task_input.default_value

        for task_input in self.task_input_schema:
            val = self.task_input_values.get(task_input.name)

            if val is None:
                continue

            if task_input.allow_parameters:
                val = utils.replace_parameters_from_anything(
                    val, self.pipeline_parameters
                )

            parsed_input_values[task_input.name] = val

        # Validate inputs
        for task_input in self.task_input_schema:
            if task_input.required and parsed_input_values[task_input.name] is None:
                raise ValueError(f"{task_input.name} is required but not provided")

        return parsed_input_values

    def validate_inputs(self) -> None:
        """Validate inputs.

        Basic input validation will be performed in the get_parsed_inputs() method
        but to provide more complex validation, override this method.
        """
        pass

    def set_task_inputs(self) -> None:
        """Set input values.

        Override this method to update values of variables from input variable dictionary.
        """
        pass

    def run(self, input_values: Dict[str, Any]) -> None:
        """Run the task."""
        raise NotImplementedError("Task is not implemented")
