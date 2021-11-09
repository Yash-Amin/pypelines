"""Pipeline options."""
from yaml import safe_load
from typing import Any, Dict
from datetime import datetime
from pymongo import MongoClient, DESCENDING

from pypelines import utils
from pypelines.constants import SnapshotCollectionFields
from pypelines.config import (
    DB_NAME,
    DB_CONNECTION_STRING,
    DB_SNAPSHOT_COLLECTION_NAME,
)


class PipelineOptions:
    """Class for storing pipeline options."""

    def __init__(
        self,
        pipeline_name: str = "",
        use_snapshots: bool = False,
        parameters: Dict[str, str] = {},
    ) -> None:
        """Init"""
        now = datetime.now()
        self.pipeline_start_time: str = str(now)

        self.use_snapshots: bool = utils.string_to_bool(
            utils.replace_parameters_from_anything(use_snapshots, parameters)
        )
        self.pipeline_name: str = utils.replace_parameters_from_anything(
            pipeline_name, parameters
        )

        self.parameters: Dict[str, str] = parameters

        self.pipeline_id: str = None

        # TODO: make this configurable either from yaml or command-line arguments
        self.continue_from_last_run = True

    def get_pipeline_id(self) -> str:
        """Return pipeline id

        If continue-from-last-run is enabled, returns last pipeline id.
        Else returns new pipeline id.
        """
        if not self.continue_from_last_run:
            return str(datetime.timestamp(self.now))

        db = MongoClient(DB_CONNECTION_STRING)[DB_NAME][DB_SNAPSHOT_COLLECTION_NAME]

        # Finds last snapshot with same database name as pipeline name
        last_snapshot_doc = db.findOne(
            {SnapshotCollectionFields.PIPELINE_NAME: self.pipeline_name},
            sort={(SnapshotCollectionFields.CREATED_AT, DESCENDING)},
        )

        if last_snapshot_doc is None:
            return str(datetime.timestamp(self.now))

        # If last pipeline run was completed then return new pipeline id
        if last_snapshot_doc.get(SnapshotCollectionFields.IS_COMPLETED):
            return str(datetime.timestamp(self.now))

        return last_snapshot_doc[SnapshotCollectionFields.PIPELINE_ID]

    def load(self):
        """Load pipeline options."""
        self.pipeline_id = self.get_pipeline_id()

    def load_from_file(self, file_path: str) -> None:
        """Load pipeline options from file."""
        with open(file_path, "r") as f:
            config: Dict[str, Any] = safe_load.load(f)["config"]

        self.pipeline_name = config["pipeline-name"]
        self.use_snapshots = utils.string_to_bool(
            self.config.get("use-snapshots", False)
        )

        self.load()

    def get_config_dict(self) -> Dict[str, Any]:
        """Get config dict."""
        return {
            "pipeline-id": self.pipeline_id,
            "use-snapshot": self.use_snapshots,
            "pipeline-name": self.pipeline_name,
            "start-time": self.pipeline_start_time,
        }
