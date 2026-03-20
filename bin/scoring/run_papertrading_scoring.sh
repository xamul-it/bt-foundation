#!/bin/bash
# Script per paper trading scoring (da schedulare con cron alle 8:00 AM)

# Set working directory
cd /home/htpc/jupyter

# Activate virtual environment if needed
# source venv/bin/activate

# Log file
LOG_DIR="logs"
mkdir -p $LOG_DIR
LOG_FILE="$LOG_DIR/papertrading_$(date +%Y%m%d).log"

echo "=================================================" >> $LOG_FILE
echo "Paper Trading Scoring - $(date)" >> $LOG_FILE
echo "=================================================" >> $LOG_FILE

# Run scoring generator
python3 generate_scoring_data.py \
    --mode papertrading \
    --config config_scoring.json \
    --output "scoring_data_today.csv" \
    >> $LOG_FILE 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Scoring generation completed successfully" >> $LOG_FILE

    # Optional: copy to backtrader data folder
    # cp scoring_data_today.csv /path/to/backtrader/data/

    # Optional: send notification
    # echo "Scoring data ready for today" | mail -s "Paper Trading Ready" your@email.com
else
    echo "✗ Scoring generation failed with exit code $EXIT_CODE" >> $LOG_FILE

    # Optional: send alert
    # echo "Check log: $LOG_FILE" | mail -s "Paper Trading ERROR" your@email.com
fi

echo "=================================================" >> $LOG_FILE
echo "" >> $LOG_FILE

exit $EXIT_CODE
