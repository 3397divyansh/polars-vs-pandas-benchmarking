@echo off

echo.
echo [Step 1 of 3]: Building and Starting Docker Containers...
docker compose up -d --build

echo.
echo [Step 2 of 3]: Generating 1-Million-Row E-Commerce Dataset...
python scripts/generate_ecomm_data.py

echo.
echo [Step 3 of 3]: Running the Automated Benchmark Pipeline...
python scripts/run_pipeline.py

pause