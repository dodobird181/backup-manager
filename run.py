#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from logging.handlers import TimedRotatingFileHandler
from typing import Any, List
from uuid import uuid4

import systemd.daemon

from get_backups_to_prune import should_prune

Arguments = namedtuple(
    "ArgNamespace",
    ["live", "log_level", "disable_pruning", "ignore_missing_dirs"],
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
    parser.add_argument(
        "-i",
        default=False,
        action="store_true",
        help="Ignore missing dirs and proceed with the backup without prompting. This flag is ignored when service mode is enabled.",
    )
    args = parser.parse_args()
    setattr(args, "ignore_missing_dirs", args.i)
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


def now_str() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z%z")


def format_seconds(seconds: int) -> str:
    td = timedelta(seconds=seconds)
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if days != 0:
        return f"{days}d {hours}h {minutes}m"
    if hours != 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def wait_for_confirm(message: str) -> bool:
    while True:
        response = input(f"{now_str()} {message} (y/n)?")
        if response.lower().strip() == "y":
            return True
        elif response.lower().strip() == "n":
            return False
        else:
            print("Enter (y/Y) to confirm or (n/N) to cancel.")


def run(*args, check=True, capture_output=True, text=True, **kwargs) -> subprocess.CompletedProcess:
    """Run a command with some default arguments."""
    return subprocess.run(args, text=text, check=check, capture_output=capture_output, **kwargs)


class BaseConfig:
    """
    Base config handles environment variable substution automatically for any string-typed attributes.
    """

    class InvalidConfig(Exception):
        """
        The config contains some invalid formatting or data.
        """

        ...

    class MissingEnvVar(InvalidConfig):
        """
        An environment variable required for the config is missing.
        """

        def __init__(self, varname: str):
            super().__init__(
                f"One or more variable(s) in the string '{varname}' is missing from the environment but required in 'config.yaml'."
            )

    def _expand_env_var(self, path: str) -> str:
        """
        Expand environment variables in the path and raise `MissingEnvVar` if any
        variable is missing from the environment.
        """
        if not bool(re.search(r"\$\{[^}]+\}", path)):
            # no varnames are in the path
            return path
        expanded = os.path.expandvars(path)
        if expanded == path:
            # expansion didn't happen but there is an env var pattern. This means the var is not set.
            raise self.MissingEnvVar(path)
        return expanded

    def __setattr__(self, key: str, value: Any) -> None:
        """
        Attempt to inject environment variables defined by the syntax '${VAR_NAME}'
        when string attributes are set.
        """
        if isinstance(value, str):
            expanded = self._expand_env_var(value)
            super().__setattr__(key, expanded)
        elif isinstance(value, List):
            expanded = [self._expand_env_var(x) if isinstance(x, str) else x for x in value]
            super().__setattr__(key, expanded)
        else:
            # normal setattr for all other datatypes
            super().__setattr__(key, value)


@dataclass
class Config(BaseConfig):
    """
    Python representation of the config file.
    """

    @dataclass
    class Database(BaseConfig):

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
    class FileFormat(BaseConfig):
        prefix: str
        datetime: str

    @dataclass
    class PruningStrategy(BaseConfig):
        keep_daily: int
        keep_weekly: int
        keep_monthly: int
        keep_yearly: int

    @dataclass
    class ServiceMode(BaseConfig):

        class Frequency(Enum):
            HOURLY = "hourly"
            DAILY = "daily"
            WEEKLY = "weekly"

        class Day(Enum):
            MONDAY = "monday"
            TUESDAY = "tuesday"
            WEDNESDAY = "wednesday"
            THURSDAY = "thursday"
            FRIDAY = "friday"
            SATURDAY = "saturday"
            SUNDAY = "sunday"

        enabled: bool
        frequency: Frequency
        num_hours: int
        _time_of_day: str
        day_of_week: Day

        def test_for_config(self) -> Config.ServiceMode:
            """Raise invalid config if we cannot parse for the next run time in seconds."""
            try:
                self.next_run_in(datetime.now().astimezone())
                return self
            except Exception as e:
                raise Config.InvalidConfig("Error parsing service mode.") from e

        def time_of_day_dt(self) -> datetime:
            system_tz = datetime.now().astimezone().tzinfo
            return datetime.strptime(self._time_of_day, "%H:%M").replace(tzinfo=system_tz)

        def time_of_day_str(self) -> str:
            return self.time_of_day_dt().strftime("%H:%M %Z%z")

        def _next_weekday_in(self) -> int:
            """How many days until the next scheduled weekday. If we are scheduled for today then return 7."""
            now = datetime.now().astimezone()
            dt = now
            for i in range(1, 8):
                dt += timedelta(days=1)
                weekday = self.Day(dt.strftime("%A").lower())
                if weekday == self.day_of_week:
                    return i
            raise Exception("This shouldn't happen!")

        def next_run_in(self, last_ran_at: datetime) -> float:
            """How many seconds from now the next scheduled backup will run."""
            now = datetime.now().astimezone()
            if self.Frequency.HOURLY == self.frequency:
                return (last_ran_at + timedelta(hours=int(self.num_hours))).timestamp() - now.timestamp()
            elif self.Frequency.DAILY == self.frequency:
                today_run_time = self.time_of_day_dt().replace(year=now.year, month=now.month, day=now.day)
                tomorrow_run_time = self.time_of_day_dt().replace(
                    year=now.year, month=now.month, day=now.day
                ) + timedelta(days=1)
                if last_ran_at.date() < now.date():
                    # if we last ran on a day before today, return the difference between now and the time of day we're scheduled to run.
                    return today_run_time.timestamp() - now.timestamp()
                else:
                    # We have already ran today, so return the difference between then now and the time of day we're scheduled to run tomorrow.
                    return tomorrow_run_time.timestamp() - now.timestamp()
            elif self.Frequency.WEEKLY == self.frequency:
                current_weekday = self.Day(now.strftime("%A").lower())
                if last_ran_at + timedelta(days=7) < now and current_weekday == self.day_of_week:
                    # It's been at least 7 days since the last run, and we are on the correct day of the week, so we *should* be running today.
                    # Therefore, return the today run time diff.
                    today_run_time = self.time_of_day_dt().replace(year=now.year, month=now.month, day=now.day)
                    return today_run_time.timestamp() - now.timestamp()
                else:

                    # It's been LESS than 7 days since the last run. Therefore, we should return the diff from 7 days after the last run date at the daily run time.
                    run_time = self.time_of_day_dt()
                    future_run_date = last_ran_at + timedelta(days=self._next_weekday_in())
                    future_run_dt = future_run_date.replace(
                        hour=run_time.hour,
                        minute=run_time.minute,
                    )
                    return future_run_dt.timestamp() - now.timestamp()

            else:
                raise Config.InvalidConfig(f"Unsupported service mode frequency: {self.frequency}.")

        def __str__(self) -> str:
            if self.enabled == False:
                return "Service mode disabled. Running one time only before shutting down."
            elif self.frequency == self.Frequency.HOURLY:
                return f"Service mode enabled. Backing up every {self.num_hours} hour(s)."
            elif self.frequency == self.Frequency.DAILY:
                return f"Service mode enabled. Backing up every day at {self.time_of_day_str()}."
            elif self.frequency == self.Frequency.WEEKLY:
                return f"Service mode enabled. Backing up every week on {self.day_of_week.name} at {self.time_of_day_str()}."
            raise Config.InvalidConfig(f"Unsupported service mode frequency: {self.frequency}.")

    rclone_remote: str
    file_format: FileFormat
    dirs: List[str]
    pruning: PruningStrategy
    databases: List[Database]
    logdir: str
    service_mode: ServiceMode


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
            service_mode=Config.ServiceMode(
                enabled=data["service_mode"]["enabled"],
                frequency=Config.ServiceMode.Frequency(data["service_mode"]["frequency"].lower()),
                num_hours=data["service_mode"]["num_hours"],
                _time_of_day=data["service_mode"][
                    "time_of_day"
                ],  # _ because the datetime str is parsed  + formatted lazily
                day_of_week=Config.ServiceMode.Day(data["service_mode"]["day_of_week"].lower()),
            ).test_for_config(),
        )

    except (KeyError, ValueError) as e:
        raise Config.InvalidConfig from e


def refresh_current_backups_from_rclone(config: Config):
    with open(f"{parent_path()}/current_backups.txt", "w") as f:
        subprocess.run(["rclone", "lsf", config.rclone_remote], stdout=f)


class BackupRunner:

    class DirNotFound(Exception):
        """
        The backup directory was not found.
        """

        def __init__(self, dirname: str):
            self.dirname = dirname
            super().__init__(
                "The directory '{}' was not found. Paths checked: [{}, {}]".format(
                    dirname,
                    dirname,
                    f"{os.getcwd()}/{dirname}",
                )
            )

    class SkipDir(Exception):
        """
        The backup directory was not found and will be skipped.
        """

        def __init__(self, dirname: str):
            self.dirname = dirname
            super().__init__(f"Skipping the directory: {dirname}.")

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

    def _parse_dirname(self, dirname: str) -> str:
        """
        Make sure the directory name exists on the machine. This method tries
        the following in order: 1.) absolute path, and 2.) relative path from the
        parent directory to where `run.py` is located, i.e., the current working
        directory when running the script. Will raise an exception if the directory
        doesn't exist and otherwise return the guaranteed path to the directory.
        """
        if os.path.exists(dirname):
            return dirname
        elif os.path.exists(f"{os.getcwd()}/{dirname}"):
            return f"{os.getcwd()}/{dirname}"
        elif get_arguments().ignore_missing_dirs == True or self.config.service_mode.enabled == True:
            # Ignore missing directories if "-i" flag passed or we are running in service mode (since a daemon
            # shouldn't prompt the CLI for input to make a decision.)
            raise self.SkipDir(dirname)
        raise self.DirNotFound(dirname)

    def run(self) -> None:
        LIVE = bool(get_arguments().live)
        BASIC_CLI_TOOLS = ["rclone", "zip", "yq", "pv"]
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
            try:
                parsed_dirname = self._parse_dirname(dirname)
            except self.DirNotFound:
                backup = wait_for_confirm(
                    f"[WARNING]: Could not find directory '{dirname}'. Do you wish to proceed"
                )
                if backup == False:
                    self.logger.info("Backup cancelled.")
                    exit(0)
            except self.SkipDir:
                continue

        # Check for basic linux dependencies and load the config file
        self._check_dependencies(BASIC_CLI_TOOLS)
        # Check if the user needs to install any Postgres specific cli tools
        if any([db.provider == Config.Database.Provider.POSTGRES for db in config.databases]):
            self._check_dependencies(PG_CLI_TOOLS)

        # Check database connections exist
        for db in config.databases:
            if db.test_conn() == False:
                self.logger.error(f"Backup failed. Could not connect to database: {db}.")
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
            try:
                parsed_dirname = self._parse_dirname(dirname)
                self.logger.debug(f"Copying {dirname}...")
                run("mkdir", "-p", f"{BACKUP_WORKSPACE}/{dirname}")
                run("cp", "-r", parsed_dirname, f"{BACKUP_WORKSPACE}/{dirname}", capture_output=False)
            except (self.SkipDir, self.DirNotFound):
                self.logger.warning(f"Skipping dir '{dirname}' because it was not found.")

        prefix = config.file_format.prefix
        prefix = "" if prefix.strip() == "" else f"{prefix}_"
        COMPRESSED_BACKUP_PATH = f"{parent_dir()}/{prefix}{BACKUP_TIMESTAMP}.zip"
        self.logger.info(f"Compressing files to {COMPRESSED_BACKUP_PATH}...")
        # zip -r - folder/ | pv -s $(du -sb folder/ | awk '{print $1}')
        du_result = run("du", "-sb", f"{parent_dir()}/{ZIP_DIR}").stdout
        num_bytes = int(du_result.split()[0])
        zip_proc = subprocess.Popen(
            # "-" = write zip to stdout
            ["zip", "-rq", "-", f"{parent_dir()}/{ZIP_DIR}"],
            stdout=subprocess.PIPE,
        )
        with open(COMPRESSED_BACKUP_PATH, "wb") as out_file:
            # piped output to progress viewer to see progress bar
            pv_proc = subprocess.Popen(["pv", "-s", str(num_bytes)], stdin=zip_proc.stdout, stdout=out_file)
            if zip_proc.stdout:
                # let zip_proc get SIGPIPE if pv exits
                zip_proc.stdout.close()
            pv_proc.communicate()
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

                # Find backups to prune
                with open(f"{parent_path()}/current_backups.txt", "r") as f:
                    filenames = [line.strip() for line in f if line.strip()]
                to_prune = should_prune(
                    filenames=filenames,
                    file_format=f"{prefix}{config.file_format.datetime}.zip",
                    keep_daily=config.pruning.keep_daily,
                    keep_weekly=config.pruning.keep_weekly,
                    keep_monthly=config.pruning.keep_monthly,
                    keep_yearly=config.pruning.keep_yearly,
                )
                with open(PRUNE_FILE, "w") as f:
                    for fn in to_prune:
                        f.write(fn + "\n")
                self.logger.debug(f"Prune list written to {PRUNE_FILE}. {len(to_prune)} files to prune.")

                # Prune backups
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

    if os.getcwd() == parent_path():
        raise Config.InvalidConfig(
            "Backup failed. Please make sure you're running the program from your project's root directory."
        )

    config = load_configuration()
    runner = BackupRunner(config)
    runner.logger.info(config.service_mode)

    if config.service_mode.enabled == True:
        systemd.daemon.notify("READY=1")  # daemon is ready after config is loaded
        # Run periodically
        last_ran_at = datetime.now().astimezone() - timedelta(weeks=(52 * 20))  # 20 years back.. arbitrary
        while True:
            next_run_in = config.service_mode.next_run_in(last_ran_at)
            if next_run_in < 0:
                try:
                    runner.run()
                    last_ran_at = datetime.now().astimezone()
                    next_run_in = config.service_mode.next_run_in(last_ran_at)
                    runner.logger.info(f"Next run in {format_seconds(int(next_run_in))}. Going to sleep...")
                except Exception as e:
                    runner.logger.error("Backup failed.", exc_info=e)
            time.sleep(30)  # check against service mode schedule around every 30 seconds
    else:
        # Run once...
        try:
            runner.run()
        except Exception as e:
            runner.logger.error("Backup failed.", exc_info=e)
