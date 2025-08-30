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
        default=False,
        action="store_true",
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
            return f"<{self.provider.name} Database - {self.name}@{self.host}:{self.port}>"

        def conn_args(self) -> List[str]:
            """Connection arguments for the database."""
            if self.provider == Config.Database.Provider.POSTGRES:
                return ["-h", db.host, "-p", db.port, "-U", db.username, "-d", db.name]
            raise Config.Database.UnsupportedProvider(db.provider)

        def test_conn(self) -> bool:
            if self.provider == Config.Database.Provider.POSTGRES:
                os.environ["PGPASSWORD"] = db.password  # Needed to avoid prompting for a password
                result = run("psql", *self.conn_args(), "-c", "\\q", check=False)
                if result.returncode == 0:
                    return True
                return False
            raise Config.Database.UnsupportedProvider(self.provider)

        def dump(self, path: str) -> subprocess.CompletedProcess:
            if self.provider == Config.Database.Provider.POSTGRES:
                os.environ["PGPASSWORD"] = db.password  # Needed to avoid prompting for a password
                with open(f"{path}.sql", "w") as dump_file:
                    return run("pg_dump", *self.conn_args(), stdout=dump_file, capture_output=False)
            raise Config.Database.UnsupportedProvider(self.provider)

    def pretty_print(self) -> None:
        data = {
            "remote": config.rclone_remote,
            "directories": config.dirs,
            "databases": [str(d) for d in config.databases],
            "pruning": "Keep {} daily, {} weekly, {} monthly, and {} yearly backups.".format(
                config.pruning.keep_daily,
                config.pruning.keep_weekly,
                config.pruning.keep_monthly,
                config.pruning.keep_yearly,
            ),
        }
        log(f"Loaded config: {json.dumps(data, indent=2)}")

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


def refresh_current_backups_from_rclone(config: Config):
    with open(f"{parent_path()}/current_backups.txt", "w") as f:
        subprocess.run(["rclone", "lsf", config.rclone_remote], stdout=f)


if __name__ == "__main__":

    args = get_arguments()

    LIVE = bool(args.live)
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
    config.pretty_print()

    # Check if the user needs to install any Postgres specific cli tools
    if any([db.provider == Config.Database.Provider.POSTGRES for db in config.databases]):
        check_dependencies(PG_CLI_TOOLS)

    # Check database connections exist
    for db in config.databases:
        if db.test_conn() == False:
            log(f"Backup failed. Could not connect to database: {db}.")
            exit(1)

    # Backup the databases
    BACKUP_WORKSPACE = f"{parent_path()}/{ZIP_DIR}"
    BACKUP_TIMESTAMP = datetime.now().astimezone().strftime(config.file_format.datetime)
    run("mkdir", BACKUP_WORKSPACE)
    log(f"Created temporary workspace: '{BACKUP_WORKSPACE}'.")
    for i, db in enumerate(sorted(config.databases, key=lambda db: db.name)):
        # use index to make sure names are unique and throw in db name for identification
        dump_path = f"{BACKUP_WORKSPACE}/{BACKUP_TIMESTAMP}_{db.name}_{i}"
        db.dump(dump_path)

    log(f"Copying backup directories {config.dirs}...")
    for dirname in config.dirs:
        run("cp", "-r", f"{os.getcwd()}/{dirname}", f"{BACKUP_WORKSPACE}/{dirname}", capture_output=False)

    prefix = config.file_format.prefix
    prefix = "" if prefix.strip() == "" else f"{prefix}_"
    COMPRESSED_BACKUP_PATH = f"{parent_dir()}/{prefix}{BACKUP_TIMESTAMP}.zip"
    log(f"Compressing files to {COMPRESSED_BACKUP_PATH}...")
    run(
        "zip",
        "-r",
        COMPRESSED_BACKUP_PATH,  # relative path so zip doesn't include nested directories
        f"{parent_dir()}/{ZIP_DIR}",
    )
    run("rm", "-rf", BACKUP_WORKSPACE, capture_output=False)

    if LIVE:
        log(f"Uploading backup to rclone remote: '{config.rclone_remote}'.")
        run("rclone", "copy", COMPRESSED_BACKUP_PATH, config.rclone_remote, capture_output=False)
    else:
        log("Skipping upload to rclone because the '--live' flag is false.")
    run("rm", "-rf", COMPRESSED_BACKUP_PATH)

    PRUNE_FILE = f"{parent_path()}/to_prune.txt"
    if LIVE:
        log("Pruning old backup files...")
        refresh_current_backups_from_rclone(config)
        run(
            "python",
            f"{parent_path()}/get_backups_to_prune.py",
            f"--input-file={parent_path()}/current_backups.txt",
            f"--output-file={PRUNE_FILE}",
            f"--file-format={prefix}{config.file_format.datetime}.zip",
            f"--keep-daily={config.pruning.keep_daily}",
            f"--keep-weekly={config.pruning.keep_weekly}",
            f"--keep-monthly={config.pruning.keep_monthly}",
            f"--keep-yearly={config.pruning.keep_yearly}",
        )
        with open(PRUNE_FILE, "r") as prune_file:
            for backup_filename in prune_file.readlines():
                backup_filename = backup_filename.replace("\n", "")
                log(f"Pruning {backup_filename}...")
                run("rclone", "delete", f"{config.rclone_remote}/{backup_filename}", capture_output=False)
    else:
        log("Skipping pruning because the '--live' flag is false.")
    run("rm", "-rf", PRUNE_FILE)

    # refresh again to update the backups list after backup is complete
    refresh_current_backups_from_rclone(config)

    log("Done!")
