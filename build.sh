#!/usr/bin/env bash
# exit on error
set -o errexit

# Install system dependencies
apt-get update
apt-get install -y python3-dev build-essential

# Install Python packages
pip install --upgrade pip
pip install -r requirements.txt 