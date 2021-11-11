"""Script Task."""
from typing import Any, Dict, List

from pypelines.pipeline_options import PipelineOptions
from pypelines.task import PipelineTask, TaskInputSchema


class ScriptTask(PipelineTask):
    """Script task."""

    task_type: str = "script"

    task_input_schema: List[TaskInputSchema] = [
        TaskInputSchema(
            name="ignore-script-erros",
            default_value=False,
            description="If true, then pipeline will not fail if script terminates with non-zero exit code",
        ),
        TaskInputSchema(
            name="show-output",
            default_value=True,
            description="If true, then script's output will be shown in the terminal",
        ),
        TaskInputSchema(
            name="script",
            description="Script that you want to run.",
            allow_parameters=False,
        ),
        TaskInputSchema(
            name="arguments",
            description="List of arguments.",
            default_value=[],
        ),
        TaskInputSchema(
            name="environment-variables",
            description="Environment variables for script",
            default_value={},
        ),
        TaskInputSchema(
            name="use-snapshots",
            description="Use snapshots",
            default_value=True,
        ),
    ]

    def __init__(
        self,
        name: str,
        task_input_values: Dict[str, Any],
        pipeline_parameters: Dict[str, Any],
        pipeline_options: PipelineOptions,
    ) -> None:
        super().__init__(
            name,
            task_input_values,
            pipeline_parameters,
            pipeline_options,
        )

        self.script = ""
        self.show_output = True
        self.use_snapshots = True
        self.ignore_task_errors = False
        self.arguments = []
        self.script_environment_variables: Dict[str, str] = {}

    def validate_inputs(self):
        """Validate inputs"""
        pass

    def set_task_inputs(self) -> None:
        """Sets task inputs"""
        inputs = super().get_parsed_inputs()

        task_input_dict = {key: value for key, value in inputs.items()}

        self.script = task_input_dict["script"]
        self.show_output = task_input_dict["show-output"]
        self.use_snapshots = task_input_dict["use-snapshots"]
        self.ignore_task_errors = task_input_dict["ignore-script-erros"]
        self.arguments = task_input_dict["arguments"]
        self.script_environment_variables = task_input_dict["environment-variables"]

        if self.use_snapshots is None:
            # if use_snapshots is not set, then set it to pipeline_options.use_snapshots
            self.use_snapshots = self.pipeline_options.use_snapshots
        else:
            # use_snapshot will be true only if it is set to true and
            # pipeline-options.use_snapshots is true as well
            self.use_snapshots = (
                self.pipeline_options.use_snapshots and self.use_snapshots
            )

    def run(
        self,
    ) -> None:
        """Run script task."""
        self.set_task_inputs()

        # TODO: run script
        print("Running - ", self.name)
        print("Script - ", self.script)
        print("Arguments - ", self.arguments)
