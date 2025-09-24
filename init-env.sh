#!/bin/bash

# Exit on error
set -e

# Create venv if it doesn't exist
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# Activate venv
source venv/bin/activate

# Upgrade pip
pip3 install --upgrade pip
pip3 install -r requirements.txt
echo "Virtual environment initialized and pip upgraded."