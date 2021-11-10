"""Main"""
import os
import argparse
from typing import List
from pypelines import utils

from pypelines.pipeline import Pipeline


def parse_arguments() -> argparse.Namespace:
    """Parses arguments"""
    parser = argparse.ArgumentParser(description="Pypelines - Tasks runner")

    parser.add_argument(
        "-pipeline-path", help="Path to pipeline yml file", required=True
    )

    parser.add_argument(
        "-parameters",
        help="Add parameters in this format - '-parameters param1=value1 param2=value2'",
        nargs="*",
    )

    parser.add_argument(
        "-continue-from-last-run",
        help="Continue from last run",
        action="store_true",
        default=False,
    )

    parser.add_argument("-debug", help="Debug mode", action="store_true", default=False)

    args = parser.parse_args()

    pipeline_path = args.pipeline_path

    if not os.path.isfile(pipeline_path):
        raise FileNotFoundError(f"Pipeline file {pipeline_path} not found")

    return args


def main() -> None:
    args = parse_arguments()

    pipeline_path: str = args.pipeline_path
    raw_parameters: List[str] = args.parameters
    continue_from_last_run: bool = args.continue_from_last_run
    debug: bool = args.debug

    # Parse parameters
    parameters = utils.get_parameters_from_string_arguments(raw_parameters)

    # Create pipeline
    pipeline: Pipeline = Pipeline()

    # TODO: use continue_from_last_run and debug flags
    pipeline.load_from_yaml(pipeline_path, parameters)


if __name__ == "__main__":
    main()
