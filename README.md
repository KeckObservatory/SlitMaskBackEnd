# SlitMaskBackend

DatabaseAPI
  Required files to run:
    * slitmask_cfg.live.ini
    * wspgcfg_live.py

Scripts:
  Emails,  required updates: slitmask_emails.ini 
  Backups,  required files:  backup_metabase.live.conf


Database Configuration:

The database is set-up to have the data_directory as specified in /var/lib/pgsql/data/postgresql.conf 
and the account information is defined in /var/lib/pgsql/data/pg_hba.conf

Back-ups are made once a day as specified in the crontab.
