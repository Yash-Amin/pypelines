"""Task to run for each file matching glob pattern."""
import os
from glob import glob
import concurrent.futures
from typing import Any, Dict, List

from pypelines.utils import string_to_bool
from pypelines.validation import output_parameter_name
from pypelines.pipeline_options import PipelineOptions
from pypelines.task import PipelineTask, TaskInputSchema

# Task input keys
INPUT_GLOB_PATTERN = "glob-pattern"
INPUT_THREADS = "threads"
INPUT_OUTPUT_PARAMETER_NAME = "output-parameter-name"
INPUT_INCLUDE_SUBDIRECTORIES = "include-subdirectories"
INPUT_SUB_TASKS_CONFIG = "tasks"


class ForEachFileTask(PipelineTask):
    """Task to run given sub-tasks for each file matching glob pattern."""

    task_type: str = "for-each-file"

    task_input_schema: List[TaskInputSchema] = [
        TaskInputSchema(
            name=INPUT_GLOB_PATTERN,
            description="Provide glob pattern to match files",
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
            value_type=output_parameter_name,
        ),
        TaskInputSchema(
            name=INPUT_INCLUDE_SUBDIRECTORIES,
            description=(
                "If true, files of subdirectories matching glob pattern will be included."
            ),
            value_type=string_to_bool,
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

        self.glob_pattern: str = None
        self.output_parameter_name: str = None
        self.include_subdirectories: bool = None
        self.threads: int = 1

    def set_task_inputs(self) -> None:
        """Set task inputs."""
        inputs = super().get_parsed_inputs()

        self.glob_pattern = os.path.expanduser(inputs[INPUT_GLOB_PATTERN])
        self.output_parameter_name = inputs[INPUT_OUTPUT_PARAMETER_NAME]
        self.include_subdirectories = inputs[INPUT_INCLUDE_SUBDIRECTORIES]
        self.threads = inputs[INPUT_THREADS]
        self.sub_tasks = inputs[INPUT_SUB_TASKS_CONFIG]

    def _run_sub_tasks(self, file_path: str, thread_state: Dict[str, Any]) -> None:
        """Run sub-tasks."""
        print(f"Running sub-tasks for {file_path}")

        # TODO: check snapshot

        from pypelines.tasks import run_task

        # For each given task, run it with file_path as extra_parameters
        for task_config in self.sub_tasks:
            try:
                run_task(
                    task_config=task_config,
                    parameters=self.parameters,
                    pipeline_options=self.pipeline_options,
                    extra_parameters={
                        **self._extra_parameters,
                        self.output_parameter_name: file_path,
                    },
                )
            except Exception as e:
                thread_state["error"] = True
                thread_state["exceptions"].append(e)
                raise e

    def run(self) -> None:
        """Run task."""
        self.set_task_inputs()

        # Get list of files matching glob pattern
        list_files = glob(self.glob_pattern, recursive=self.include_subdirectories)

        # To store error in any thread
        threads_state = {"error": False, "exceptions": []}

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.threads
        ) as executor:
            futures = []
            for file_path in list_files:
                futures.append(
                    executor.submit(self._run_sub_tasks, file_path, threads_state)
                )

        if threads_state["error"]:
            raise Exception(f"Error in sub-tasks. {threads_state['exceptions']}")

        # TODO: Save snapshots
