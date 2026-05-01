#!/bin/bash

docker compose up -d --build

python3 scripts/generate_ecomm_data.py

python3 scripts/run_pipeline.py
