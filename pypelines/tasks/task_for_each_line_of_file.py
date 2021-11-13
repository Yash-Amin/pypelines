"""Task to run sub-tasks for each line of file."""
import os
import concurrent.futures
from typing import Any, Dict, List

from pypelines.utils import string_to_bool
from pypelines.pipeline_options import PipelineOptions
from pypelines.task import PipelineTask, TaskInputSchema

# Task input keys
INPUT_FILE_PATH = "file-path"
INPUT_THREADS = "threads"
INPUT_TRIM_LINES = "trim-lines"
INPUT_SKIP_EMPTY_LINES = "skip-empty-lines"
INPUT_OUTPUT_PARAMETER_NAME = "output-parameter-name"
INPUT_SUB_TASKS_CONFIG = "tasks"


class ForEachLineOfFileTask(PipelineTask):
    """Task to run sub-tasks for each line of file."""

    task_type: str = "for-each-line-of-file"

    task_input_schema: List[TaskInputSchema] = [
        TaskInputSchema(
            name=INPUT_FILE_PATH,
            description="Provide the input file path.",
        ),
        TaskInputSchema(
            name=INPUT_THREADS,
            description="Provide number of threads for running sub-tasks.",
            value_type=int,
            default_value=1,
        ),
        TaskInputSchema(
            name=INPUT_OUTPUT_PARAMETER_NAME,
            description=(
                "Provide name of the output parameter."
                "A parameter with given name will be passed to sub-tasks "
                "and can be accessed using ${{parameters.OUTPUT_PARAMETER_NAME}}"
            ),
        ),
        TaskInputSchema(
            name=INPUT_TRIM_LINES,
            description=(
                "If true, line values will be trimmed before storing in output parameter."
            ),
            value_type=string_to_bool,
            default_value=True,
        ),
        TaskInputSchema(
            name=INPUT_SKIP_EMPTY_LINES,
            description="If true, empty lines will be skipped.",
            value_type=string_to_bool,
            default_value=True,
        ),
        TaskInputSchema(
            name=INPUT_SUB_TASKS_CONFIG,
            description="Provide tasks to run a file.",
            value_type=list,
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

        self.file_path: str = None
        self.threads: int = 1
        self.trim_lines: bool = True
        self.skip_empty_lines: bool = True
        self.output_parameter_name: str = None
        self.sub_tasks: list = []

    def set_task_inputs(self) -> None:
        """Set task inputs."""
        inputs = super().get_parsed_inputs()

        self.file_path = inputs[INPUT_FILE_PATH]
        self.threads = inputs[INPUT_THREADS]
        self.trim_lines = inputs[INPUT_TRIM_LINES]
        self.skip_empty_lines = inputs[INPUT_SKIP_EMPTY_LINES]
        self.output_parameter_name = inputs[INPUT_OUTPUT_PARAMETER_NAME]
        self.sub_tasks = inputs[INPUT_SUB_TASKS_CONFIG]

    def _run_sub_tasks(self, current_line: str, thread_state: Dict[str, Any]) -> None:
        """Run sub-tasks."""
        print(f"Running sub-tasks for line '{current_line}'")

        # TODO: check snapshot

        from pypelines.tasks import run_task

        # Run subtasks with extra parameters containing current_line value
        for task_config in self.sub_tasks:
            try:
                run_task(
                    task_config=task_config,
                    parameters=self.parameters,
                    pipeline_options=self.pipeline_options,
                    extra_parameters={
                        **self._extra_parameters,
                        self.output_parameter_name: current_line,
                    },
                )
            except Exception as e:
                thread_state["error"] = True
                thread_state["exceptions"].append(e)
                raise e

        # TODO: save snapshot

    def run(self) -> None:
        """Run task."""
        self.set_task_inputs()

        # TODO: check snapshot

        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(
                f"File {self.file_path} not found for '{self.name}' task."
            )

        # Read lines from file
        lines: List[str] = []
        with open(self.file_path, "r") as f:
            _lines = f.read().splitlines()

            # Trim lines
            _lines = [s.strip() for s in _lines] if self.trim_lines else _lines

            # Remove empty
            lines = [s for s in _lines if s] if self.skip_empty_lines else _lines

        # To store error in any thread
        threads_state = {"error": False, "exceptions": []}

        # Run sub-tasks
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.threads
        ) as executor:
            futures = []
            for line in lines:
                futures.append(
                    executor.submit(self._run_sub_tasks, line, threads_state)
                )

        if threads_state["error"]:
            raise Exception(f"Error in sub-tasks. {threads_state['exceptions']}")

        # TODO: save snapshot
