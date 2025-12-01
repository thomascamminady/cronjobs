# Cronjobs



```
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of week (0 - 6) (Sunday=0 or 7)
# │ │ │ │ │
# │ │ │ │ │
# * * * * *  command to execute

# Run cleanup every 4 hours
0 */4 * * * /Users/thomascamminady/Dev/cronjobs/cleanup_dmg.sh

# Run merge_parquet_files.py every hour at minute 10
*/15 * * * * /opt/homebrew/bin/uv run /Users/thomascamminady/Dev/cronjobs/process_rungap.py >> /tmp/run_gap.log 2>&1
```
