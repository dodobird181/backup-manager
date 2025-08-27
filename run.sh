#!/bin/bash

function now(){
    date '+%Y-%m-%d %H:%M:%S %Z'
}

# Send stdout and stderr to logfiles
mkdir "backups/logs" >/dev/null 2>&1
logfiles=( "backups/logs/$(now).txt" "backups/logs/latest.txt" )
for filename in "${logfiles[@]}"; do
    exec > >(tee "$filename")
    exec 2> >(tee "$filename" >&2)
done

echo "$(now) - Running backup..."

###
# Check script preconditions...
###
required_tools=(
    rclone
    pg_dump
    zip
    python3
    yq  # see backups README for info on yq
)
for toolname in "${required_tools[@]}"; do
    if [ -z "$(which $toolname)" ]; then
        echo "$(now) - Backup failed. Please install and configure $toolname..."
        exit 1
    fi
done

yaml_get() {
    local yaml_file="backups/config.yaml"
    local key_path="$1"
    local __resultvar="$2"
    local is_list="$3"   # pass "true" if it's a list

    if [[ "$is_list" == "true" ]]; then
        # Read YAML list into a Bash array
        local tmp
        mapfile -t tmp < <(yq e -r "${key_path}[]" "$yaml_file")

        # Expand env vars for each element
        for i in "${!tmp[@]}"; do
            tmp[$i]=$(eval echo "${tmp[$i]}")
            if [[ -z "${tmp[$i]}" ]]; then
                echo "$(now) - Backup failed. Empty value in list '$key_path' at index $i."
                exit 1
            fi
        done

        # Assign array to the provided variable name
        eval "$__resultvar=(\"\${tmp[@]}\")"
    else
        # Single value (string) logic
        local config_val
        config_val=$(yq e "${key_path}" "$yaml_file")

        if [[ "$config_val" =~ ^\$\{(.+)\}$ ]]; then
            local var_name="${BASH_REMATCH[1]}"
            local value="${!var_name}"

            if [[ -z "$value" ]]; then
                echo "$(now) - Backup failed. Environment variable '$var_name' is not set or empty."
                exit 1
            fi
            eval "$__resultvar=\"$value\""
        else
            if [[ -z "$config_val" ]]; then
                echo "$(now) - Backup failed. YAML key '$key_path' is empty."
                exit 1
            fi
            eval "$__resultvar=\"$config_val\""
        fi
    fi
}

yaml_get '.backup.rclone_remote' RCLONE_REMOTE_NAME
yaml_get '.backup.datetime_format' DATETIME_FORMAT
yaml_get '.backup.prefix' BACKUP_FILE_PREFIX
yaml_get '.backup.dirs' BACKUP_DIRS true

yaml_get '.backup.pruning.keep_daily' KEEP_DAILY
yaml_get '.backup.pruning.keep_weekly' KEEP_WEEKLY
yaml_get '.backup.pruning.keep_monthly' KEEP_MONTHLY
yaml_get '.backup.pruning.keep_yearly' KEEP_YEARLY

yaml_get '.backup.postgres.name' DB_NAME
yaml_get '.backup.postgres.username' USERNAME
yaml_get '.backup.postgres.password' PASSWORD
yaml_get '.backup.postgres.host' HOST
yaml_get '.backup.postgres.port' PORT

# Check postgres connection
if ! pg_isready -h "$HOST" -p "$PORT" -U "$USERNAME" >/dev/null 2>&1; then
    echo "$(now) - Backup failed. Cannot connect to Postgres at $HOST:$PORT. Is the database running?"
    exit 1
fi

###
# Perform the backup...
###
zip_preparation_dir=".tmp_backup_workspace"
mkdir $zip_preparation_dir

echo "$(now) - Dumping Postgres database..."
official_backup_timestamp="$(date +"$DATETIME_FORMAT")"
DB_BACKUP_FILE="$zip_preparation_dir/postgres_rush_db_backup_$official_backup_timestamp.sql"
export PGPASSWORD="$PASSWORD" # Needed for pg_dump to avoid prompting for a password
pg_dump -h "$HOST" -p "$PORT" -U "$USERNAME" "$DB_NAME" > "$DB_BACKUP_FILE"

echo "$(now) - Copying files..."
backup_directories=(
    DJANGO_STATIC_ROOT
    DJANGO_MEDIA_ROOT
    DEPLOY_LOGS_DIR
)
for path in "${BACKUP_DIRS[@]}"; do
    if [ -d "$path" ]; then
        mkdir -p "$zip_preparation_dir/$path"
        cp -r "$path" "$zip_preparation_dir/$path"
    else
        echo "$(now) - WARNING: Missing directory: '$path'. Skipping during backup."
    fi
done

echo "$(now) - Compressing files..."
COMPRESSED_BACKUP_FILE="$BACKUP_FILE_PREFIX$official_backup_timestamp.zip"
zip -r "$COMPRESSED_BACKUP_FILE" "$zip_preparation_dir/" >/dev/null
rm -r "$zip_preparation_dir"

echo "$(now) - Uploading backup file '$COMPRESSED_BACKUP_FILE'..."
rclone copy $COMPRESSED_BACKUP_FILE $RCLONE_REMOTE_NAME
rm "$COMPRESSED_BACKUP_FILE"

# Back off saving older backup versions to save storage space.
echo "$(now) - Purging old backup files..."
rclone lsf $RCLONE_REMOTE_NAME > backups/current_backups.txt
python backups/get_backups_to_prune.py \
    --input-file=backups/current_backups.txt \
    --output-file=backups/to_prune.txt \
    --file-format="$BACKUP_FILE_PREFIX$DATETIME_FORMAT.zip" \
    --keep-daily="$KEEP_DAILY" \
    --keep-weekly="$KEEP_WEEKLY" \
    --keep-monthly="$KEEP_MONTHLY" \
    --keep-yearly="$KEEP_YEARLY" \
    >/dev/null

#rm "backups/current_backups.txt"
to_prune=( $(cat "backups/to_prune.txt") )
for filename in "${to_prune[@]}"; do
    echo "$(now) - Pruning $filename..."
    rclone delete "$RCLONE_REMOTE_NAME$filename"
done
rm "backups/to_prune.txt"

# Update current backup list
rclone lsf $RCLONE_REMOTE_NAME > backups/current_backups.txt

echo "$(now) - Done!"
