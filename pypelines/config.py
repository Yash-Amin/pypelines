"""Config vars"""
import os


WORKSPACE_DIRECTORY = os.path.expanduser("~/.Pypelines/")

# Tmp directory to store scripts
SCRIPTS_DIRECTORY = os.path.join(WORKSPACE_DIRECTORY, "snapshots")

# DB connection string
DATABASE_CONNECTION_STRING = os.environ.get(
    "DATABASE_CONNECTION_STRING", "mongodb://localhost:27017/"
)

# Database name
DB_NAME = os.environ.get("DATABASE_NAME", "Pypelines")

# Snapshots collection name
DB_SNAPSHOT_COLLECTION_NAME = os.environ.get("DB_SNAPSHOT_COLLECTION_NAME", "Snapshots")

# Create directories
for dir_path in [SCRIPTS_DIRECTORY]:
    os.makedirs(dir_path, exist_ok=True)
