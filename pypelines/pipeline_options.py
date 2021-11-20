"""Pipeline options."""
from typing import Any, Dict
from datetime import datetime
from pymongo import DESCENDING

from pypelines import utils
from pypelines.snapshot import Snapshot
from pypelines.constants import SnapshotCollectionFields


class PipelineOptions:
    """Class for storing pipeline options."""

    def __init__(self) -> None:
        """Init."""
        self.now = datetime.now()
        self.pipeline_start_time: str = str(self.now)

        self.pipeline_id: str = None
        self.pipeline_name: str = None
        self.use_snapshots: bool = False
        self.parameters: Dict[str, str] = None

        # When pipeline is completed, set this to True
        self.is_completed: bool = False

        # TODO: see if this should be used or not
        # Update this to true when any task fails, so at the end when all
        # tasks are completed, the pipeline is not marked as completed.
        self.any_task_failed: bool = False

        # TODO: make this configurable either from yaml or command-line arguments
        self.continue_from_last_run = True

        self.snapshot: Snapshot = None

    def get_pipeline_id(self) -> str:
        """Return pipeline id.

        If continue-from-last-run is enabled, returns last pipeline id.
        Else returns current timestamp as new pipeline ID.
        """
        new_pipeline_id = f"{datetime.timestamp(self.now)} - {self.pipeline_name}"
        if not self.continue_from_last_run:
            return new_pipeline_id

        db = utils.get_snapshots_collection()

        # Finds last snapshot with same database name as pipeline name
        last_snapshot_doc = db.find_one(
            dict(pipeline_name=self.pipeline_name),
            sort=[("created_at", DESCENDING)],
        )

        if last_snapshot_doc is None:
            return new_pipeline_id

        # If last pipeline run was completed then return new pipeline id
        if last_snapshot_doc.get(SnapshotCollectionFields.IS_COMPLETED):
            return new_pipeline_id

        return last_snapshot_doc["_id"]

    def load(self, config: Dict[str, Any], parameters: Dict[str, Any]) -> None:
        """Load pipeline options."""
        self.parameters = parameters

        self.pipeline_name = utils.replace_parameters_from_anything(
            config["name"], parameters
        )

        self.use_snapshots = utils.string_to_bool(
            utils.replace_parameters_from_anything(
                config.get("use-snapshots", False), parameters
            )
        )

        self.pipeline_id = self.get_pipeline_id()

        self.snapshot = Snapshot(self.pipeline_id, self.pipeline_name)

        # Insert new snapshot doc if it does not exist
        self.snapshot.create_if_not_exist()

    def get_config_dict(self) -> Dict[str, Any]:
        """Get config dict."""
        return {
            "name": self.pipeline_name,
            "pipeline-id": self.pipeline_id,
            "use-snapshot": self.use_snapshots,
            "start-time": self.pipeline_start_time,
            "is-completed": self.is_completed,
        }
