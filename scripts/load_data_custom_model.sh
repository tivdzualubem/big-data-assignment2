#!/bin/bash
set -e

cd "$(dirname "$0")/.."

echo "Starting PostgreSQL..."
sudo service postgresql start || true
sleep 3

echo "Activating Python environment..."
source venv/bin/activate

echo "Running custom scalable model loader..."
python scripts/load_data_custom_model.py

echo "Custom scalable model load completed."
