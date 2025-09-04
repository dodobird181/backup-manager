# Dodo Backup Manager

A simple program for backing up files and databases written by Samuel Morris using Python and [Rclone](https://rclone.org/).

The motivation behind this project was to create an easy way to configure *what* to back up, *where* the data should be sent, and *how long* it should persist there. All you need to do to create a new backup "system" is [configure an rclone remote](https://rclone.org/docs/#configure), specify which directories and databases to back up in `config.yaml`, and either run the program periodically or as a service by setting `service_mode.enabled=true` in the config.

Currently, only [PostgreSQL](https://www.postgresql.org/) and [SQLite](https://sqlite.org/) databses are supported.

## Installation:
1. Clone this repository to a new folder on your machine. You can name the folder whatever you want, but I usually choose something like "backups" or "backup_manager".
2. Run `apt install python3 python-is-python3 zip pv && python --version` to install Python3, zip, and pv (progress viewer).
3. Install [yq](https://github.com/mikefarah/yq?tab=readme-ov-file#install) by following their README.
4. Copy `config.yaml.template` and rename the copy to `config.yaml`.
4. Run `./[your_folder_name]/run.py` to do a dry-run of a backup. You should see an output similar to:
```
2025-08-31_02-53-08_PM_EDT-0400 [INFO]: Starting backup...
2025-08-31_02-53-08_PM_EDT-0400 [INFO]: Created temporary workspace: '/home/dodo/Documents/github/backup-manager/45b9ae92547d46c8a9e558ff874d9747_tmp_backup_manager_workspace'.
2025-08-31_02-53-08_PM_EDT-0400 [INFO]: Copying backup directories []...
2025-08-31_02-53-08_PM_EDT-0400 [INFO]: Compressing files to backup-manager/some_prefix__2025-08-31_02-53-08_PM.zip...
2025-08-31_02-53-08_PM_EDT-0400 [INFO]: Skipping upload to rclone because the '--live' flag is false.
2025-08-31_02-53-08_PM_EDT-0400 [INFO]: Skipping pruning because the '--live' flag is false.
2025-08-31_02-53-09_PM_EDT-0400 [INFO]: Skipping refresh because the '--live' flag is false.
2025-08-31_02-53-09_PM_EDT-0400 [INFO]: Done!
```
5. Now that the dry-run has succeeded. Install and configure `rclone` with a remote, see [their documentation](https://rclone.org/docs/#configure), so that you have somewhere to back-up files to. If you haven't yet chosen a cloud storage provider, I would recommend [Backblaze](https://www.backblaze.com/). Rclone is a crazy powerful tool so I would highly recommend checking out what you can do with their remotes.
6. Set the `rclone.remote` in `config.yaml` to the remote name you just configured. NOTE: if you are not specifying a bucket or more specific location on the remote, it should have a ":" suffix at the end, e.g., "remote_name:".
7. Run `./[your_folder_name]/run.py --live` to communicate with the Rclone remote.
8. That's it! The `config.yaml.template` file has examples and comments that explain how to configure the backup manager properly.

**WARNING**: This program expects the folder you save backups to on your cloud storage provider to ONLY contain the specifically-formatted backup files created by it. There are some consequences of this decision. For example, you should not configure more than one backup-manager to point to the same remote if they have different prefixes or timestamp formats. And you should also not change the prefix or timestamp format once you have started saving backups. If you really want/need to change the prefix or timestamp format you should make sure this program is not currently running, temporarily unschedule this program, rename all the current backup files according to the new prefix and timestamp format, change the prefix and timestamp format in your `config.yaml` file, and, finally, reschedule this program.

**TIP**: If you're backing up a Postgres database make sure your `pg_dump` version matches your Postgres database. Postgres is picky about version mismatches.
