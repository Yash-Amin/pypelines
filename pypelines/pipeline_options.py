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

    def __init__(self) -> None:
        """Init."""
        self.now = datetime.now()
        self.pipeline_start_time: str = str(self.now)

        self.pipeline_id: str = None
        self.pipeline_name: str = None
        self.use_snapshots: bool = False
        self.parameters: Dict[str, str] = None

        # TODO: make this configurable either from yaml or command-line arguments
        self.continue_from_last_run = True

    def get_pipeline_id(self) -> str:
        """Return pipeline id.

        If continue-from-last-run is enabled, returns last pipeline id.
        Else returns new pipeline id.
        """
        if not self.continue_from_last_run:
            return str(datetime.timestamp(self.now))

        db = MongoClient(DB_CONNECTION_STRING)[DB_NAME][DB_SNAPSHOT_COLLECTION_NAME]

        # Finds last snapshot with same database name as pipeline name
        last_snapshot_doc = db.find_one(
            {SnapshotCollectionFields.PIPELINE_NAME: self.pipeline_name},
            sort=[(SnapshotCollectionFields.CREATED_AT, DESCENDING)],
        )

        if last_snapshot_doc is None:
            return str(datetime.timestamp(self.now))

        # If last pipeline run was completed then return new pipeline id
        if last_snapshot_doc.get(SnapshotCollectionFields.IS_COMPLETED):
            return str(datetime.timestamp(self.now))

        return last_snapshot_doc[SnapshotCollectionFields.PIPELINE_ID]

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

    def get_config_dict(self) -> Dict[str, Any]:
        """Get config dict."""
        return {
            "name": self.pipeline_name,
            "pipeline-id": self.pipeline_id,
            "use-snapshot": self.use_snapshots,
            "start-time": self.pipeline_start_time,
        }
