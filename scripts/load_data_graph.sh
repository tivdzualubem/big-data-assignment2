#!/bin/bash
set -e

cd "$(dirname "$0")/.."

echo "Starting Neo4j..."
sudo service neo4j start || true
sleep 5

echo "Activating Python environment..."
source venv/bin/activate

echo "Running Neo4j Python loader..."
python scripts/load_data_graph.py

echo "Neo4j load completed."
