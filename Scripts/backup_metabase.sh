#!/bin/bash

# script to run daily backups of the metabase database.
# to run,  update the backup_metabase.live.conf with the server parameters

# read the config
source ./backup_metabase.live.conf

# get the date,  set the file name.
TIMESTAMP=$(date +"%Y%m%d_%Hh%Mm")
BACKUP_FILE="$BACKUP_DIR/metabase_$TIMESTAMP.sql"

# dump
PGPASSWORD=$PSQL_PASSWORD pg_dump -U $PSQL_USER $PSQL_DATABASE > $BACKUP_FILE

# zip
gzip $BACKUP_FILE
