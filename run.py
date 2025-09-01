#!/usr/bin/env python3

import argparse
import json
import logging
import os
import subprocess
import sys
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache
from logging.handlers import TimedRotatingFileHandler
from typing import List
from uuid import uuid4

Arguments = namedtuple(
    "ArgNamespace",
    ["live", "log_level", "disable_pruning"],
)


def get_arguments() -> Arguments:
    """Parse script arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--live",
        default=False,
        action="store_true",
        help="Send API calls to rclone when the live flag is true. Otherwise this program will only run locally and output logs.",
    )
    parser.add_argument(
        "--disable-pruning",
        default=False,
        action="store_true",
        help="Do not prune old backups.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str,
        help="The console log level. One of: [DEBUG, INFO, WARNING, ERROR]. ",
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
            print("Enter (y/Y) to confirm or (n/N) to cancel.")


def run(*args, check=True, capture_output=True, text=True, **kwargs) -> subprocess.CompletedProcess:
    """Run a command with some default arguments."""
    return subprocess.run(args, text=text, check=check, capture_output=capture_output, **kwargs)


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
            SQLITE = "sqlite"

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
            if self.provider == Config.Database.Provider.POSTGRES:
                return f"<{self.provider.name} Database - {self.name}@{self.host}:{self.port}>"
            elif self.provider == Config.Database.Provider.SQLITE:
                return f"<{self.provider.name} Database - '{self.name}'>"
            raise Config.Database.UnsupportedProvider(self.provider)

        def conn_args(self) -> List[str]:
            """Connection arguments for the database."""
            if self.provider == Config.Database.Provider.POSTGRES:
                return ["-h", self.host, "-p", self.port, "-U", self.username, "-d", self.name]
            elif self.provider == Config.Database.Provider.SQLITE:
                return ["ls", "-lh", self.name]
            raise Config.Database.UnsupportedProvider(self.provider)

        def test_conn(self) -> bool:
            if self.provider == Config.Database.Provider.POSTGRES:
                os.environ["PGPASSWORD"] = self.password  # Needed to avoid prompting for a password
                result = run("psql", *self.conn_args(), "-c", "\\q", check=False)
                if result.returncode == 0:
                    return True
                return False
            elif self.provider == Config.Database.Provider.SQLITE:
                result = run(*self.conn_args(), check=False)
                if result.returncode == 0:
                    return True
                return False
            raise Config.Database.UnsupportedProvider(self.provider)

        def dump(self, path: str) -> subprocess.CompletedProcess:
            if self.provider == Config.Database.Provider.POSTGRES:
                os.environ["PGPASSWORD"] = self.password  # Needed to avoid prompting for a password
                with open(f"{path}.sql", "w") as dump_file:
                    return run("pg_dump", *self.conn_args(), stdout=dump_file, capture_output=False)
            elif self.provider == Config.Database.Provider.SQLITE:
                return run("cp", self.name, f"{path}.db")
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
    logdir: str


def load_configuration() -> Config:
    result = run("yq", "-e", "-o=json", ".", config_path())
    try:
        data = json.loads(result.stdout)["backup"]
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
                *[
                    Config.Database(
                        provider=Config.Database.Provider.POSTGRES,
                        name=db["name"],
                        host=db["host"],
                        port=db["port"],
                        username=db["username"],
                        password=db["password"],
                    )
                    for db in (data["databases"]["postgres"] if data["databases"]["postgres"] else [])
                ],
                *[
                    # Database name is just the sqlite file path here, and everything else is left blank.
                    Config.Database(
                        provider=Config.Database.Provider.SQLITE,
                        name=db["path"],
                        host="",
                        port="",
                        username="",
                        password="",
                    )
                    for db in (data["databases"]["sqlite"] if data["databases"]["sqlite"] else [])
                ],
            ],
            logdir=data["logs"]["dir"],
        )

    except KeyError as e:
        raise Config.InvalidConfig from e


def refresh_current_backups_from_rclone(config: Config):
    with open(f"{parent_path()}/current_backups.txt", "w") as f:
        subprocess.run(["rclone", "lsf", config.rclone_remote], stdout=f)


class BackupRunner:

    def __init__(self, config: Config):
        self.config = config
        self.logger = self._logger_from_config(config)

    def _logger_from_config(self, config: Config) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(get_arguments().log_level)
        run("mkdir", "-p", f"{parent_dir()}/{config.logdir}")
        file_handler = TimedRotatingFileHandler(
            f"{parent_dir()}/{config.logdir}/log.txt",
            when="midnight",  # rotate once per day at midnight
            interval=30,  # ~monthly rotation
            backupCount=12,
            utc=True,
        )
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s]: %(message)s",
            datefmt="%Y-%m-%d_%I-%M-%S_%p_%Z%z",
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        return logger

    def _check_dependencies(self, tools):
        for toolname in tools:
            result = run("which", toolname, check=False).stdout
            if result.strip() == "":
                self.logger.error(f"Backup failed. Please install and configure: '{toolname}'.")
                exit(1)

    def run(self) -> None:
        LIVE = bool(get_arguments().live)
        BASIC_CLI_TOOLS = ["rclone", "zip", "yq"]
        PG_CLI_TOOLS = ["pg_dump", "psql"]
        ZIP_DIR = f"{uuid4().hex}_tmp_backup_manager_workspace"

        self.logger.info("Starting backup...")

        # Log the config data in DEBUG
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
        self.logger.debug(f"Loaded config: {json.dumps(data, indent=2)}")

        # warn if directory to backup is missing
        for dirname in self.config.dirs:
            if not dir_exists(dirname):
                backup = wait_for_confirm(f"WARNING: Could not find directory '{dirname}'. Do you wish to proceed")
                if backup == False:
                    self.logger.info("Backup cancelled.")
                    exit(0)

        if os.getcwd() == parent_path():
            self.logger.info(
                "Backup failed. Please make sure you're running the program from your project's root directory."
            )
            exit(1)

        # Check for basic linux dependencies and load the config file
        self._check_dependencies(BASIC_CLI_TOOLS)

        # Check if the user needs to install any Postgres specific cli tools
        if any([db.provider == Config.Database.Provider.POSTGRES for db in config.databases]):
            self._check_dependencies(PG_CLI_TOOLS)

        # Check database connections exist
        for db in config.databases:
            if db.test_conn() == False:
                self.logger.info(f"Backup failed. Could not connect to database: {db}.")
                exit(1)

        # Backup the databases
        BACKUP_WORKSPACE = f"{parent_path()}/{ZIP_DIR}"
        BACKUP_TIMESTAMP = datetime.now().astimezone().strftime(config.file_format.datetime)
        run("mkdir", BACKUP_WORKSPACE)
        self.logger.info(f"Created temporary workspace: '{BACKUP_WORKSPACE}'.")
        for i, db in enumerate(sorted(config.databases, key=lambda db: db.name)):
            # use index to make sure names are unique and throw in db name for identification
            self.logger.info(f"Dumping database {db}...")
            dump_path = f"{BACKUP_WORKSPACE}/{BACKUP_TIMESTAMP}_{db.name.replace('/', '_').replace('.', '_')}_{i}"
            db.dump(dump_path)

        self.logger.info(f"Copying backup directories {config.dirs}...")
        for dirname in config.dirs:
            run("cp", "-r", f"{os.getcwd()}/{dirname}", f"{BACKUP_WORKSPACE}/{dirname}", capture_output=False)

        prefix = config.file_format.prefix
        prefix = "" if prefix.strip() == "" else f"{prefix}_"
        COMPRESSED_BACKUP_PATH = f"{parent_dir()}/{prefix}{BACKUP_TIMESTAMP}.zip"
        self.logger.info(f"Compressing files to {COMPRESSED_BACKUP_PATH}...")
        run(
            "zip",
            "-r",
            COMPRESSED_BACKUP_PATH,  # relative path so zip doesn't include nested directories
            f"{parent_dir()}/{ZIP_DIR}",
        )
        run("rm", "-rf", BACKUP_WORKSPACE, capture_output=False)

        if LIVE:
            self.logger.info(f"Uploading backup to rclone remote: '{config.rclone_remote}'.")
            run("rclone", "copy", COMPRESSED_BACKUP_PATH, config.rclone_remote, capture_output=False)
        else:
            self.logger.info("Skipping upload to rclone because the '--live' flag is false.")
        run("rm", "-rf", COMPRESSED_BACKUP_PATH)

        if get_arguments().disable_pruning == True:
            self.logger.info("Skipping pruning because '--disable-pruning' was passed as an argument.")
        else:
            PRUNE_FILE = f"{parent_path()}/to_prune.txt"
            if LIVE:
                self.logger.info("Pruning old backup files...")
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
                        self.logger.info(f"Pruning {backup_filename}...")
                        run("rclone", "delete", f"{config.rclone_remote}/{backup_filename}", capture_output=False)
            else:
                self.logger.info("Skipping pruning because the '--live' flag is false.")
            run("rm", "-rf", PRUNE_FILE)

        # refresh again to update the backups list after backup is complete
        if LIVE:
            refresh_current_backups_from_rclone(config)
        else:
            self.logger.info("Skipping refresh because the '--live' flag is false.")

        self.logger.info("Done!")


if __name__ == "__main__":
    config = load_configuration()
    runner = BackupRunner(config)
    try:
        runner.run()
    except Exception as e:
        runner.logger.error("Backup failed.", exc_info=e)
