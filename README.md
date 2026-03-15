Polars vs Pandas FastAPI Benchmark

An empirical performance benchmark comparing **Polars** and **Pandas** in **FastAPI** microservices to evaluate latency, throughput, CPU usage, and memory consumption under concurrent workloads.

This project supports the research study:

**"Empirical Analysis of Polars and Pandas Performance for FastAPI Microservices"**

---

# Overview

Modern microservice architectures frequently process large datasets in real time. While Pandas has long been the standard Python library for data analysis, newer engines such as Polars offer improved performance through multithreading, lazy evaluation, and query optimization.

This repository provides a controlled benchmarking environment to evaluate how these two libraries perform when used inside FastAPI microservices.

The experiment compares:

* Response latency
* Throughput (requests per second)
* CPU utilization
* Memory consumption
* Scalability under concurrent load

---

# Architecture

The project consists of two identical microservices:

<pre class="overflow-visible! px-0!" data-start="1225" data-end="1487"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute end-1.5 top-1 z-2 md:end-2 md:top-1"></div><div class="pe-11 pt-3"><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>Client / Load Generator</span><br/><span>        |</span><br/><span>        |</span><br/><span>        v</span><br/><span>+----------------------+</span><br/><span>|   FastAPI Service    |</span><br/><span>|     (Pandas)         |</span><br/><span>+----------------------+</span><br/><br/><span>+----------------------+</span><br/><span>|   FastAPI Service    |</span><br/><span>|     (Polars)         |</span><br/><span>+----------------------+</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

Both services process the same dataset and expose identical API endpoints. Performance metrics are collected while the services handle concurrent requests.

All services run inside **Docker** containers to ensure a fair resource allocation environment.

---

# Tech Stack

| Component        | Technology                             |
| ---------------- | -------------------------------------- |
| API framework    | **FastAPI**                      |
| Data processing  | **Pandas**/**Polars**      |
| Load testing     | **Locust**                       |
| Containerization | **Docker**                       |
| Monitoring       | **Prometheus**+**Grafana** |
| Visualization    | **Matplotlib**                   |

---

# Project Structure

<pre class="overflow-visible! px-0!" data-start="2278" data-end="2624"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute end-1.5 top-1 z-2 md:end-2 md:top-1"></div><div class="pe-11 pt-3"><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>polars-vs-pandas-fastapi-benchmark</span><br/><br/><span>│</span><br/><span>├── pandas_service</span><br/><span>│   ├── app.py</span><br/><span>│   ├── data_processing.py</span><br/><span>│   └── requirements.txt</span><br/><span>│</span><br/><span>├── polars_service</span><br/><span>│   ├── app.py</span><br/><span>│   ├── data_processing.py</span><br/><span>│   └── requirements.txt</span><br/><span>│</span><br/><span>├── dataset_generator</span><br/><span>│   └── generate_data.py</span><br/><span>│</span><br/><span>├── load_testing</span><br/><span>│   └── locustfile.py</span><br/><span>│</span><br/><span>├── docker-compose.yml</span><br/><span>└── results</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

---

# Dataset

Synthetic datasets are generated to simulate real-world transactional workloads.

Dataset characteristics:

* 100K – 5M rows
* Multiple numeric columns
* Timestamp column
* Simulated sales transactions

Operations performed during benchmarks:

* Filtering
* Aggregation
* Group-by queries

Example query workload:

<pre class="overflow-visible! px-0!" data-start="2957" data-end="3032"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute end-1.5 top-1 z-2 md:end-2 md:top-1"></div><div class="pe-11 pt-3"><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>Filter: price > 100</span><br/><span>Group by: product_id</span><br/><span>Aggregation: SUM(quantity)</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

---

# Running the Project

## 1 Clone the Repository

<pre class="overflow-visible! px-0!" data-start="3089" data-end="3219"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute inset-x-4 top-12 bottom-4"><div class="pointer-events-none sticky z-40 shrink-0 z-1!"><div class="sticky bg-token-border-light"></div></div></div><div class=""><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span class="ͼs">git</span><span> clone https://github.com/navjot369/polars-vs-pandas-fastapi-benchmark.git</span><br/><span class="ͼs">cd</span><span> polars-vs-pandas-fastapi-benchmark</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

---

## 2 Generate Dataset

<pre class="overflow-visible! px-0!" data-start="3249" data-end="3302"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute inset-x-4 top-12 bottom-4"><div class="pointer-events-none sticky z-40 shrink-0 z-1!"><div class="sticky bg-token-border-light"></div></div></div><div class=""><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>python dataset_generator/generate_data.py</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

---

## 3 Build and Run Services

<pre class="overflow-visible! px-0!" data-start="3338" data-end="3375"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute inset-x-4 top-12 bottom-4"><div class="pointer-events-none sticky z-40 shrink-0 z-1!"><div class="sticky bg-token-border-light"></div></div></div><div class=""><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>docker-compose up </span><span class="ͼu">--build</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

Services will start at:

<pre class="overflow-visible! px-0!" data-start="3402" data-end="3483"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute end-1.5 top-1 z-2 md:end-2 md:top-1"></div><div class="pe-11 pt-3"><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>Pandas API  -> http://localhost:8001</span><br/><span>Polars API  -> http://localhost:8002</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

---

# API Endpoint

Both services expose the same endpoint:

<pre class="overflow-visible! px-0!" data-start="3547" data-end="3567"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute end-1.5 top-1 z-2 md:end-2 md:top-1"></div><div class="pe-11 pt-3"><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>GET /process</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

Example response:

<pre class="overflow-visible! px-0!" data-start="3588" data-end="3636"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute inset-x-4 top-12 bottom-4"><div class="pointer-events-none sticky z-40 shrink-0 z-1!"><div class="sticky bg-token-border-light"></div></div></div><div class=""><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>{</span><br/><span>  "rows": </span><span class="ͼq">500</span><span>,</span><br/><span>  "latency": </span><span class="ͼq">0.12</span><br/><span>}</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

---

# Load Testing

Load testing is performed using  **Locust** .

Run Locust:

<pre class="overflow-visible! px-0!" data-start="3750" data-end="3798"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute inset-x-4 top-12 bottom-4"><div class="pointer-events-none sticky z-40 shrink-0 z-1!"><div class="sticky bg-token-border-light"></div></div></div><div class=""><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>locust </span><span class="ͼu">-f</span><span> load_testing/locustfile.py</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

Open the web interface:

<pre class="overflow-visible! px-0!" data-start="3825" data-end="3854"><div class="relative w-full mt-4 mb-1"><div class=""><div class="relative"><div class="h-full min-h-0 min-w-0"><div class="h-full min-h-0 min-w-0"><div class="border border-token-border-light border-radius-3xl corner-superellipse/1.1 rounded-3xl"><div class="h-full w-full border-radius-3xl bg-token-bg-elevated-secondary corner-superellipse/1.1 overflow-clip rounded-3xl lxnfua_clipPathFallback"><div class="pointer-events-none absolute end-1.5 top-1 z-2 md:end-2 md:top-1"></div><div class="pe-11 pt-3"><div class="relative z-0 flex max-w-full"><div id="code-block-viewer" dir="ltr" class="q9tKkq_viewer cm-editor z-10 light:cm-light dark:cm-light flex h-full w-full flex-col items-stretch ͼk ͼy"><div class="cm-scroller"><div class="cm-content q9tKkq_readonly"><span>http://localhost:8089</span></div></div></div></div></div></div></div></div></div><div class=""><div class=""></div></div></div></div></div></pre>

Test parameters:

* Concurrent users
* Spawn rate
* Request duration

---

# Performance Metrics

The following metrics are collected during experiments:

### Latency

* Average response time
* P95 latency
* P99 latency

### Throughput

* Requests per second (RPS)

### Resource Utilization

* CPU usage
* Memory consumption

---

# Experiment Setup

Experiments are performed under varying conditions.

### Dataset Sizes

* 100K rows
* 1M rows
* 5M rows

### Concurrent Users

* 10 users
* 50 users
* 100 users
* 500 users

### Query Complexity

* Simple filtering
* Aggregation
* Group-by queries

---

# Expected Results

Based on architectural differences:

| Metric            | Expected Winner |
| ----------------- | --------------- |
| Latency           | Polars          |
| Throughput        | Polars          |
| CPU Efficiency    | Polars          |
| Memory Efficiency | Polars          |

Polars benefits from:

* multithreaded execution
* SIMD optimizations
* lazy query evaluation

---

# Results Visualization

Performance results can be visualized using **Matplotlib** graphs such as:

* Latency vs Concurrent Users
* Throughput vs Concurrent Users
* CPU Usage vs Dataset Size
* Memory Usage vs Dataset Size

---

# Research Context

This project accompanies the research study:

**Empirical Analysis of Polars and Pandas Performance for FastAPI Microservices**

Authors:

* Bhavik Sharma
* Divyansh Saini
* Navjot Singh

Mentor:

Prof. Dr. S. B. Goyal

(Chitkara University Institute of Engineering & Technology)

---

# License

This project is released under the MIT License.
