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

# Get the absolute path of the CSV file
CSV_FILE="ETF Tickers CIK_SERIES_6_16_25 - CIK_SERIES.csv"
CSV_PATH="$(pwd)/$CSV_FILE"
TARGET_DIR="/opt/render/project/src"

echo "CSV file path: $CSV_PATH"
echo "Target directory: $TARGET_DIR"

# Ensure the CSV file exists
if [ ! -f "$CSV_PATH" ]; then
    echo "Error: CSV file not found at $CSV_PATH"
    echo "Current directory contents:"
    ls -la
    exit 1
fi

# Create target directory if it doesn't exist
echo "Creating target directory..."
mkdir -p "$TARGET_DIR"

# Copy the CSV file
echo "Copying CSV file..."
cp "$CSV_PATH" "$TARGET_DIR/"

# Verify the file was copied
echo "Verifying file copy..."
ls -l "$TARGET_DIR/$CSV_FILE"

# Print contents of target directory
echo "Contents of target directory:"
ls -la "$TARGET_DIR"

echo "Build process completed. Starting application..."
gunicorn app:app 