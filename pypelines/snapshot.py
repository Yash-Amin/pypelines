"""Snapshot class for pipeline."""
from datetime import datetime

from pypelines.utils import get_snapshots_collection


class Snapshot:
    """Snapshot class for pipeline."""

    def __init__(self, pipeline_id: str, pipeline_name: str):
        """Initialize snapshot."""
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name

    def is_pipeline_completed(self) -> bool:
        """Checks if pipeline is completed"""
        snapshot_doc = get_snapshots_collection().find_one(dict(_id=self.pipeline_id))
        if snapshot_doc is None:
            return False

        return snapshot_doc.get("is_completed")

    def set_pipeline_completed(self):
        """Sets pipeline as completed"""
        get_snapshots_collection().update_one(
            dict(_id=self.pipeline_id),
            {"$set": dict(is_completed=True)},
        )

    def create_if_not_exist(self):
        """Creates snapshot if it does not exist."""
        client = get_snapshots_collection()

        old_snapshot = client.count_documents(dict(_id=self.pipeline_id), limit=1)

        if old_snapshot == 0:
            get_snapshots_collection().insert_one(
                dict(
                    _id=self.pipeline_id,
                    pipeline_name=self.pipeline_name,
                    is_completed=False,
                    created_at=datetime.now(),
                )
            )

    def is_task_completed(self, task_hash: str) -> bool:
        """Checks if task is completed"""
        return (
            get_snapshots_collection().count_documents(
                dict(_id=self.pipeline_id, completed_tasks={"$in": [task_hash]}),
                limit=1,
            )
            != 0
        )

    def set_task_completed(self, task_hash: str):
        """Sets task as completed"""
        get_snapshots_collection().update_one(
            dict(_id=self.pipeline_id),
            {"$push": dict(completed_tasks=task_hash)},
        )
