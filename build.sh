#!/usr/bin/env bash
# exit on error
set -o errexit

echo "Starting build process..."

# Print current directory and list files
echo "Current directory: $(pwd)"
echo "Files in current directory:"
ls -la

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Ensure the CSV file exists
if [ ! -f "etf_ticker_cik_series_6_16_25.csv" ]; then
    echo "Error: CSV file not found in current directory"
    exit 1
fi

# Create target directory if it doesn't exist
echo "Creating target directory..."
mkdir -p /opt/render/project/src

# Copy the CSV file
echo "Copying CSV file..."
cp etf_ticker_cik_series_6_16_25.csv /opt/render/project/src/

# Verify the file was copied
echo "Verifying file copy..."
ls -l /opt/render/project/src/etf_ticker_cik_series_6_16_25.csv

echo "Build process completed. Starting application..."
gunicorn app:app 