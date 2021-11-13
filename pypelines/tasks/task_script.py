"""Task to run scripts."""
import os
import stat
import tempfile
import subprocess
from typing import Any, Dict, List

from pypelines.utils import string_to_bool
from pypelines.config import SCRIPTS_DIRECTORY
from pypelines.pipeline_options import PipelineOptions
from pypelines.task import PipelineTask, TaskInputSchema

# Task input keys
INPUT_IGNORE_SCRIPT_ERRORS = "ignore-script-erros"
INPUT_SHOW_OUTPUT = "show-output"
INPUT_SCRIPT = "script"
INPUT_ARGUMENTS = "arguments"
INPUT_ENVIRONMENT_VARIABLES = "environment-variables"
INPUT_USE_SNAPSHOTS = "use-snapshots"


class ScriptTask(PipelineTask):
    """Script task."""

    task_type: str = "script"

    task_input_schema: List[TaskInputSchema] = [
        TaskInputSchema(
            name=INPUT_IGNORE_SCRIPT_ERRORS,
            description="If true, then pipeline will not fail if script terminates with non-zero exit code",
            value_type=string_to_bool,
            default_value=False,
        ),
        TaskInputSchema(
            name=INPUT_SHOW_OUTPUT,
            description="If true, then script's output will be shown in the terminal",
            value_type=string_to_bool,
            default_value=True,
        ),
        TaskInputSchema(
            name=INPUT_SCRIPT,
            description="Script that you want to run.",
            allow_parameters=False,
        ),
        TaskInputSchema(
            name=INPUT_ARGUMENTS,
            description="List of arguments.",
            default_value=[],
        ),
        TaskInputSchema(
            name=INPUT_ENVIRONMENT_VARIABLES,
            description="Environment variables for script",
            default_value={},
        ),
        TaskInputSchema(
            name=INPUT_USE_SNAPSHOTS,
            description="Use snapshots",
            value_type=string_to_bool,
            default_value=True,
        ),
    ]

    def __init__(
        self,
        name: str,
        task_input_values: Dict[str, Any],
        pipeline_parameters: Dict[str, Any],
        pipeline_options: PipelineOptions,
        extra_parameters: Dict[str, Any],
    ) -> None:
        super().__init__(
            name,
            task_input_values,
            pipeline_parameters,
            pipeline_options,
            extra_parameters,
        )

        self.script = ""
        self.show_output = True
        self.use_snapshots = True
        self.ignore_script_errors = False
        self.arguments = []
        self.script_environment_variables: Dict[str, str] = {}

    def validate_inputs(self):
        """Validate inputs"""
        pass

    def set_task_inputs(self) -> None:
        """Sets task inputs"""
        inputs = super().get_parsed_inputs()

        task_input_dict = {key: value for key, value in inputs.items()}

        self.script = task_input_dict[INPUT_SCRIPT]
        self.show_output = task_input_dict[INPUT_SHOW_OUTPUT]
        self.use_snapshots = task_input_dict[INPUT_USE_SNAPSHOTS]
        self.ignore_script_errors = task_input_dict[INPUT_IGNORE_SCRIPT_ERRORS]
        self.arguments = [str(arg) for arg in task_input_dict[INPUT_ARGUMENTS]]
        self.script_environment_variables = task_input_dict[INPUT_ENVIRONMENT_VARIABLES]

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
        # TODO: snapshot check

        fd, script_path = tempfile.mkstemp(dir=SCRIPTS_DIRECTORY)

        # Gives RWX permission for the owner
        os.chmod(script_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        # Write script to tmp file
        with os.fdopen(fd, "w") as tmp_file:
            tmp_file.write(self.script)

        # Subprocess options
        subprocess_options = (
            dict()
            if self.show_output
            else dict(stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        )

        # Subprocess environment variables
        # It will contain OS environment variables, pipeline config and extra parameters
        subprocess_environment = {
            **os.environ.copy(),
            **self.pipeline_options.get_config_dict(),
            **self._extra_parameters,
        }

        # Runs the script
        process = subprocess.run(
            [script_path] + self.arguments,
            env={key: str(value) for key, value in subprocess_environment.items()},
            **subprocess_options
        )

        # Delete script
        os.unlink(script_path)

        if not self.ignore_script_errors and process.returncode != 0:
            raise Exception(
                "Script terminated with {} exit code".format(process.returncode)
            )

        # TODO: Save snapshot
