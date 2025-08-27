# Sam's Backup Program
This is a generic backup program written by Samuel Morris using rclone for a project using a postgres database.

## Prerequisites:
1. Install and configure `rclone` with a remote, see [their documentation](https://rclone.org/docs/).
2. Have a Postgres database to backup, and optionally some other local directories.
3. Install the linux pre-requisites using your package manager of choice: 
a.)`pg_dump`, `python3`, `yq`, and `zip`.
b.) NOTE: Make sure your pg_dump version matches your Postgres database. Postgres is picky about version mismatches.
c.) NOTE: There are multiple versions of `yq`. The one you want is here: https://github.com/mikefarah/yq.

## Configuration:
See `config.yaml` to configure this backup program.

## Usage:
Place the `backups/` directory inside your root project directory. 

Run the bash program from inside the top-level directory of your project using `./backups/run.sh`.

Here is an example crontab that will perform a backup every day at 4:00 AM, assuming your project exists in a directory called `/user/project`, you have some optional environment variables in `/user/project/.env` that you want exported so they can be read by `config.yaml`, and you've placed this directory in "/user/project/backups": `0 4 * * * cd /user/project && set -a && . /user/project/.env && set +a && /bin/bash /user/project/backups/run.sh`.
