#!/usr/bin/env python3

import argparse
import os
import sys
from collections import namedtuple
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, List

help = """
This script finds which timestamped backup filenames should be pruned according to some number
of daily, weekly, and monthly backups, and writes this list of files to prune to an output file.
"""

Arguments = namedtuple(
    "ArgNamespace",
    [
        "input_file",
        "output_file",
        "file_format",
        "keep_daily",
        "keep_weekly",
        "keep_monthly",
        "keep_yearly",
    ],
)


def get_arguments() -> Arguments:
    """Parse script arguments"""

    def keep_help_msg(interval_name):
        return f"Number of backups to keep at the {interval_name} interval."

    parser = argparse.ArgumentParser(description=help)
    parser.add_argument("--input-file", type=str, required=True, help="Backup file names separated by newlines.")
    parser.add_argument("--file-format", type=str, required=True, help="Expected filename datetime format.")
    parser.add_argument("--output-file", type=str, default="to_prune.txt")
    parser.add_argument("--keep-daily", type=int, default=7, help=keep_help_msg("daily"))
    parser.add_argument("--keep-weekly", type=int, default=4, help=keep_help_msg("weekly"))
    parser.add_argument("--keep-monthly", type=int, default=6, help=keep_help_msg("monthly"))
    parser.add_argument("--keep-yearly", type=int, default=2, help=keep_help_msg("yearly"))
    args = parser.parse_args()
    return args  # type: ignore


class Bucket:
    """A daterange designed to store a single object."""

    class Size(Enum):
        DAY = "day"
        WEEK = "week"
        MONTH = "month"
        YEAR = "year"

        def in_days(self) -> int:
            match self.name:
                case "DAY":
                    return 1
                case "WEEK":
                    return 7
                case "MONTH":
                    return 30
                case "YEAR":
                    return 365
                case _:
                    raise ValueError(f"Unknown bucket size: {self.name}.")

    def __init__(self, size: Size, start: datetime):
        self.size = size
        self.start = start
        self.end = start + timedelta(days=size.in_days())
        self._data = None

    def add(self, data: Any) -> None:
        self._data = data

    @property
    def is_full(self) -> bool:
        return self._data != None

    def pop(self) -> Any:
        tmp = self._data
        self._data = None
        return tmp

    def peek(self) -> Any:
        return self._data

    def __str__(self) -> str:
        return f"<{self.size.name}: {self.start} -> {self.end}>"


class BucketFactory:

    @staticmethod
    def row_of_buckets(size: Bucket.Size, n: int) -> List[Bucket]:
        """The buckets are next to each other chronologically and going
        back in time from the current datetime."""
        now = datetime.now()
        now = datetime(year=now.year, month=now.month, day=now.day)
        return [
            Bucket(
                size=size,
                start=now - timedelta(days=size.in_days() * i),
            )
            for i in range(0, n)
        ]


def should_prune(
    filenames,
    file_format,
    keep_daily,
    keep_weekly,
    keep_monthly,
    keep_yearly,
) -> List[str]:
    """
    Determine which filenames to prune based on retention rules.
    Returns a list of filenames to prune.
    """

    # Parse timestamps
    files_with_ts = [(fn, datetime.strptime(fn, file_format)) for fn in filenames]
    files_with_ts = [(fn, ts) for fn, ts in files_with_ts if ts]

    # Sort newest -> oldest
    files_with_ts.sort(key=lambda x: x[1])

    # Init buckets to place backups in
    daily_buckets = BucketFactory.row_of_buckets(size=Bucket.Size.DAY, n=keep_daily)
    weekly_buckets = BucketFactory.row_of_buckets(size=Bucket.Size.WEEK, n=keep_weekly)
    monthly_buckets = BucketFactory.row_of_buckets(size=Bucket.Size.MONTH, n=keep_monthly)
    yearly_buckets = BucketFactory.row_of_buckets(size=Bucket.Size.YEAR, n=keep_yearly)

    prunable = []
    for filename, timestamp in files_with_ts:
        # For each filename, see if it's timestamp fits neatly into any empty bucket. If no such buckets
        # are empty, or exist with the correct date-range, then the filename is added to the purge list.

        added = False
        for bucket in [*daily_buckets, *weekly_buckets, *monthly_buckets, *yearly_buckets]:
            if not bucket.is_full and bucket.start <= timestamp and bucket.end >= timestamp:
                bucket.add(filename)
                added = True
                break

        if added == False:
            prunable.append(filename)

    return prunable


if __name__ == "__main__":

    args = get_arguments()
    print(args)

    if not os.path.exists(args.input_file):
        print(f"Error: Couldn't find input file '{args.input_file}'.")
        sys.exit(1)

    with open(args.input_file, "r") as f:
        filenames = [line.strip() for line in f if line.strip()]

    to_prune = should_prune(
        filenames=filenames,
        file_format=args.file_format,
        keep_daily=args.keep_daily,
        keep_weekly=args.keep_weekly,
        keep_monthly=args.keep_monthly,
        keep_yearly=args.keep_yearly,
    )

    with open(args.output_file, "w") as f:
        for fn in to_prune:
            f.write(fn + "\n")

    print(f"Prune list written to {args.output_file}. {len(to_prune)} files to prune.")
