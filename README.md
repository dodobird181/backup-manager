# Sam's Backup Program
This is a generic backup program written by Samuel Morris using rclone. Currently this backup program is a little inflexible and only works on projects that are using a Postgres database. However, this seems like a useful piece of software so I intend to make it a little more flexible in the future.

## Prerequisites:
1. Install and configure `rclone` with a remote, see [their documentation](https://rclone.org/docs/).
2. Have a Postgres database to backup, and optionally some other local directories.
3. Install the linux pre-requisites using your package manager of choice: 
a.)`pg_dump`, `python3`, `yq`, and `zip`.
b.) NOTE: Make sure your pg_dump version matches your Postgres database. Postgres is picky about version mismatches.
c.) NOTE: There are multiple versions of `yq`. The one you want is here: https://github.com/mikefarah/yq.

## Configuration:
The `config.yaml` file is used to control the backup behavior of this program. It supports environment variable substitution using the `${VAR_NAME}` format, so sensitive values or environment-specific paths can be injected dynamically.

There are some general configuration options that control the basic backup behaviour. `rclone_remote` controls which remote to save the backup files to. `prefix` is a prefix for your backup file names. `datetime_format` specifies the datetime format used in your backup file names. And, `dirs` controls which directories you wish to back up.

In addition to the general configuration options, there are two subsections: `pruning` and `postgres`.
1. `pruning` controls how many backups are retained over time:
    - keep_daily: Number of daily backups to keep.
    - keep_weekly: Number of weekly backups to keep.
    - keep_monthly: Number of monthly backups to keep.
    - keep_yearly: Number of yearly backups to keep.

2. And `postgres` controls which database to include in the backup. You must specify the database `name`, `username`, `password`, `host`, and `port`.

## Usage:

1. Create a `backups/` directory inside your root project directory.
2. Clone this repository into `backups/`.
3. Configure backups using `config.yaml`.
4. And finally, run the bash program from inside the top-level directory of your project using `./backups/run.sh`. You should see a "Done!" message at the end if everything went according to plan.

*TIP*: Here is an example crontab that will perform a backup every day at 4:00 AM, assuming your project exists in a directory called `/user/project` and you have some optional environment variables in `/user/project/.env` that you want exported so they can be read by `config.yaml`: 
```
0 4 * * * cd /user/project && set -a && . /user/project/.env && set +a & /bin/bash /user/project/backups/run.sh
```
