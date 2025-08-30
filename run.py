#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache
from pprint import pp
from typing import Any, Dict, List
from uuid import uuid4

help = """

"""

Arguments = namedtuple(
    "ArgNamespace",
    ["live"],
)


def get_arguments() -> Arguments:
    """Parse script arguments"""
    parser = argparse.ArgumentParser(description=help)
    parser.add_argument(
        "--live",
        type=bool,
        default=False,
        help="Send API calls to rclone when the live flag is true. Otherwise this program will only run locally and output logs.",
    )
    args = parser.parse_args()
    return args  # type: ignore


@lru_cache(maxsize=1)
def parent_path() -> str:
    script_path = os.path.abspath(__file__)
    return os.path.dirname(script_path)


@lru_cache(maxsize=1)
def parent_dir() -> str:
    script_path = os.path.abspath(__file__)
    parent_path = os.path.dirname(script_path)
    return os.path.basename(parent_path)


def config_path() -> str:
    return f"{parent_dir()}/config.yaml"


def dir_exists(dirname: str) -> bool:
    return os.path.exists(f"{os.getcwd()}/{dirname}")


def now_str() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z%z")


def wait_for_confirm(message: str) -> bool:
    while True:
        response = input(f"{now_str()} - {message} (y/n)?")
        if response.lower().strip() == "y":
            return True
        elif response.lower().strip() == "n":
            return False
        else:
            log("Enter (y/Y) to confirm or (n/N) to cancel.")


def log(message: str) -> None:
    print(f"{now_str()} - {message}")


def run(*args, check=True, capture_output=True, text=True, **kwargs) -> subprocess.CompletedProcess:
    """Run a command with some default arguments."""
    return subprocess.run(args, text=text, check=check, capture_output=capture_output, **kwargs)


def check_dependencies(tools):
    for toolname in tools:
        result = run("which", toolname).stdout
        if result.strip() == "":
            log(f"Backup failed. Please install and configure: '{toolname}'.")
            exit(1)


@dataclass
class Config:
    """
    Python representation of the config file.
    """

    class InvalidConfig(Exception):
        """The config contains some invalid formatting or data."""

        ...

    @dataclass
    class Database:

        class UnsupportedProvider(Exception):
            def __init__(self, provider):
                super().__init__(f"The database provider '{provider}' is not yet supported.")

        class Provider(Enum):
            POSTGRES = "postgres"

        provider: Provider
        name: str
        host: str
        port: str
        username: str
        password: str

        def __repr__(self) -> str:
            return "Config.Database(provider='{}', name='{}', host='{}', username='{}', password='{}')".format(
                self.provider,
                self.name,
                self.host,
                self.username,
                "*****",  # password redacted
            )

        def __str__(self) -> str:
            return f"<{self.provider.name} Database - {self.host}:{self.port} - {self.name}>"

        def conn_args(self) -> List[str]:
            """Connection arguments for the database."""
            if self.provider == Config.Database.Provider.POSTGRES:
                return ["-h", db.host, "-p", db.port, "-U", db.username, "-d", db.name]
            raise Config.Database.UnsupportedProvider(db.provider)

        def test_conn(self) -> bool:
            if self.provider == Config.Database.Provider.POSTGRES:
                os.environ["PGPASSWORD"] = db.password  # Needed for pg_dump to avoid prompting for a password
                code = run("psql", *self.conn_args(), "-c", "\\q", check=False).returncode
                if code == 0:
                    return True
                return False
            raise Config.Database.UnsupportedProvider(self.provider)

        def dump(self, path: str) -> None:
            if self.provider == Config.Database.Provider.POSTGRES:
                run("pg_dump", *self.conn_args(), ">", path)
            raise Config.Database.UnsupportedProvider(self.provider)

    @dataclass
    class FileFormat:
        prefix: str
        datetime: str

    @dataclass
    class PruningStrategy:
        keep_daily: int
        keep_weekly: int
        keep_monthly: int
        keep_yearly: int

    rclone_remote: str
    file_format: FileFormat
    dirs: List[str]
    pruning: PruningStrategy
    databases: List[Database]


def load_configuration() -> Config:
    result = run("yq", "-e", "-o=json", ".", config_path())
    try:
        data = json.loads(result.stdout)["backup"]

        # warn if directory to backup is missing
        for dirname in data["dirs"]:
            if not dir_exists(dirname):
                backup = wait_for_confirm(f"WARNING: Could not find directory '{dirname}'. Do you wish to proceed")
                if backup == False:
                    log("Backup cancelled.")
                    exit(0)

        return Config(
            rclone_remote=data["rclone"]["remote"],
            file_format=Config.FileFormat(
                prefix=data["format"]["prefix"],
                datetime=data["format"]["datetime"],
            ),
            dirs=data["dirs"],
            pruning=Config.PruningStrategy(
                keep_daily=data["pruning"]["keep_daily"],
                keep_weekly=data["pruning"]["keep_weekly"],
                keep_monthly=data["pruning"]["keep_monthly"],
                keep_yearly=data["pruning"]["keep_yearly"],
            ),
            databases=[
                Config.Database(
                    provider=Config.Database.Provider.POSTGRES,
                    name=db["name"],
                    host=db["host"],
                    port=db["port"],
                    username=db["username"],
                    password=db["password"],
                )
                for db in data["databases"]["postgres"]
            ],
        )

    except KeyError as e:
        raise Config.InvalidConfig from e


if __name__ == "__main__":

    args = get_arguments()

    LIVE = args.live
    BASIC_CLI_TOOLS = ["rclone", "zip", "yq"]
    PG_CLI_TOOLS = ["pg_dump", "psql"]
    ZIP_DIR = f"{uuid4().hex}_tmp_backup_manager_workspace"

    log("Starting backup...")

    if os.getcwd() == parent_path():
        log("Backup failed. Please make sure you're running the program from your project's root directory.")
        exit(1)

    # Check for basic linux dependencies and load the config file
    check_dependencies(BASIC_CLI_TOOLS)
    config = load_configuration()
    pp(config)

    # Check if the user needs to install any Postgres specific cli tools
    if any([db.provider == Config.Database.Provider.POSTGRES for db in config.databases]):
        check_dependencies(PG_CLI_TOOLS)

    # Check database connections exist
    for db in config.databases:
        if db.test_conn() == False:
            log(f"Backup failed. Could not connect to database: {db}.")
            exit(1)

    # Backup the databases
    BACKUP_WORKSPACE_PATH = f"{parent_path()}/{ZIP_DIR}"
    OFFICIAL_BACKUP_TIMESTAMP = datetime.now().astimezone().strftime(config.file_format.datetime)
    log(f"Creating temporary workspace: '{BACKUP_WORKSPACE_PATH}'...")
    run("mkdir", BACKUP_WORKSPACE_PATH)
    for db in config.databases:
        dump_path = f"{BACKUP_WORKSPACE_PATH}/{config.file_format.prefix}_{OFFICIAL_BACKUP_TIMESTAMP}_{db.name}"
        db.dump(dump_path)

    # if "postgres" in config["databases"]:
    #     ...
    # for pg_database in config["backup"]["postgres"]:
    #     ...

    log("Done!")
