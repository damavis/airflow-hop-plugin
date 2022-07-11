#!/bin/bash
rm .coverage  || echo "No previous coverage files found"
pytest --cov=airflow_hop --cov-report xml tests --ignore=tests/integration
