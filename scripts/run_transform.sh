#!/bin/bash

# Set up environment variables
export SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export LOG_FILE="/tmp/scholarship_data/cron_transform.log"

# Create necessary directories
mkdir -p /tmp/scholarship_data/raw
mkdir -p "$(dirname "$LOG_FILE")"

# Check if AWS credentials are set
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Error: AWS credentials not set. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables."
    exit 1
fi

# Activate virtual environment if it exists
if [ -d "$SCRIPT_DIR/venv" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

# Install required packages if not already installed
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    pip install -r "$SCRIPT_DIR/requirements.txt"
fi

# Log start time
echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting DAAD transformation job" >> "$LOG_FILE"

# Run the transformation script
python3 "$SCRIPT_DIR/transform_daad.py" 2>&1 | tee -a "$LOG_FILE"

# Check if the transformation was successful
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - DAAD transformation completed successfully" >> "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - DAAD transformation failed" >> "$LOG_FILE"
    exit 1
fi

# Deactivate virtual environment if it was activated
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi
