"""Contains tasks and methods related to pipeline tasks."""
from typing import Any, Dict

from pypelines import utils
from pypelines.task import PipelineTask
from pypelines.tasks.task_script import ScriptTask
from pypelines.tasks.task_for_each_file import ForEachFileTask
from pypelines.pipeline_options import PipelineOptions

# Contains all registered tasks
tasks: Dict[str, PipelineTask] = {}


def _add_task(task: PipelineTask):
    """Add a task to the task registry. Task names must be unique."""
    if task.task_type in tasks:
        raise ValueError(f"Task {task.task_type} already exists.")

    tasks[task.task_type] = task


# Add tasks in the tasks dictionary
for task in [ScriptTask, ForEachFileTask]:
    _add_task(task)


def run_task(
    task_config: Dict[str, Any],
    parameters: Dict[str, Any],
    pipeline_options: PipelineOptions,
    extra_parameters: Dict[str, Any],
):
    """Runs task."""
    task_type = task_config["task"]
    if task_type not in tasks:
        raise ValueError("Task of '{}' type was not found.".format(task_type))

    task_name = utils.replace_parameters_from_string(task_config["name"], parameters)

    task_input_values = task_config.get("inputs", {})

    task: PipelineTask = tasks[task_type](
        name=task_name,
        task_input_values=task_input_values,
        pipeline_parameters=parameters.copy(),
        pipeline_options=pipeline_options,
        extra_parameters=extra_parameters.copy(),
    )

    task.run()
