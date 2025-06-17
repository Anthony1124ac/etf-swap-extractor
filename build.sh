#!/usr/bin/env bash
# exit on error
set -o errexit

# Install Python dependencies
pip install -r requirements.txt

# Copy the CSV file to the correct location
cp etf_ticker_cik_series_6_16_25.csv /opt/render/project/src/

# Start the application
gunicorn app:app 