#!/usr/bin/env bash

# Activate the virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Deactivate the virtual environment
deactivate