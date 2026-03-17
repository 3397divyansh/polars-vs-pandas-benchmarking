

## Empirical Analysis of Polars and Pandas Performance for FastAPI Microservices 



**Team Members** 
* Divyansh Saini (2210991535)
* Navjot Singh (2210991964)
* Bhavik Sharma (2210990217)


**Software/Platform Used** 
* Docker & Docker Compose (for containerized environment constraints)
* k6 (Open-source load testing tool)
* Windows Subsystem for Linux (WSL) / Standard Linux Terminal

**Programming Language Used** 
* Python 3.10+ (API and Data Processing)
* JavaScript (k6 Load Testing Scripts)

**Steps to Run the Code** 
1. Ensure Docker Desktop is running on your machine.
2. Open a terminal in the root project directory.
3. Run the command `docker compose up -d --build` to build and start the Pandas and Polars FastAPI containers.
4. Run `python scripts/generate_ecomm_data.py` to generate the synthetic 1-million-row dataset.
5. Execute the automated benchmark pipeline by running `python scripts/run_pipeline.py`. This will automatically run the k6 load tests, monitor Docker resources, and generate performance graphs.

**Required Libraries/Tools** 
* FastAPI, Uvicorn
* Pandas, Polars, PyArrow
* Matplotlib, Seaborn (for graph generation)

**Input and Expected Output** 
* **Input:** Automated concurrent HTTP GET requests triggered via the k6 load testing tool simulating 10-20 virtual users.
* **Expected Output:** The APIs will return JSON responses containing aggregated/joined e-commerce data. The automated pipeline will ultimately output `.json` latency reports, `.csv` resource utilization logs, and `.png` benchmark graphs comparing CPU, RAM, and response times.
