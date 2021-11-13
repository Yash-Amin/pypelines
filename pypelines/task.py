"""Abstract class for PipelineTask"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

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
    # If you want to type check your inputs, set value_type in your
    # task_input_schema of the task class.
    # For example: if you want specific input to be integer only,
    # use `value_type=int`, if you want input to be boolean only,
    # use `value_type=string_to_bool`.
    value_type: Callable = None
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
        # Name of the task
        name: str,
        # input values required for given tasks
        task_input_values: Dict[str, Any],
        # Pipeline Parameters
        pipeline_parameters: Dict[str, Any],
        # PipelineOptions
        pipeline_options: PipelineOptions,
        # In _extra_parameters, extra/output parameters are stored
        # For example, some output parameters from previous parant task can be
        # passed via _extra_parameters
        _extra_parameters: Dict[str, Any],
    ) -> None:
        self.name = name
        self.pipeline_options: PipelineOptions = pipeline_options
        self.task_input_values: Dict[str, Any] = task_input_values
        self._pipeline_parameters: Dict[str, Any] = pipeline_parameters
        self._extra_parameters: Dict[str, Any] = _extra_parameters

        # Parameters, any parameter with same name in the _pipeline_parameters
        # will be orverriden by _extra_parameters
        self.parameters = {**self._pipeline_parameters, **self._extra_parameters}

    def get_parsed_inputs(self) -> Dict[str, Any]:
        """Return parsed task input values."""
        unique_input_keys = set([x.name for x in self.task_input_schema])

        # If invalid key is provided, then raise error
        for key in self.task_input_values:
            if key not in unique_input_keys:
                raise ValueError(
                    f"{key} is not a valid input for task {self.task_type}"
                )

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
                val = utils.replace_parameters_from_anything(val, self.parameters)

            if task_input.value_type is not None:
                try:
                    val = task_input.value_type(val)
                except:
                    raise Exception(
                        f"{task_input.name} is not of type {task_input.value_type}"
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

    def run(self) -> None:
        """Run the task."""
        raise NotImplementedError("Task is not implemented")
