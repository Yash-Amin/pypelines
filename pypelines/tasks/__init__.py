"""Pipeline tasks."""
from typing import Dict
from pypelines.task import PipelineTask
from pypelines.tasks.task_script import ScriptTask

tasks: Dict[str, PipelineTask] = {}


def _add_task(task: PipelineTask):
    """Add a task to the task registry. Task names must be unique."""
    if task.task_type in tasks:
        raise ValueError(f"Task {task.task_type} already exists.")

    tasks[task.task_type] = task


for task in [ScriptTask]:
    _add_task(task)
