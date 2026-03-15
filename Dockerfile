# Dockerfile
FROM python:3.11-slim

# Set environment variables to prevent Python from writing .pyc files 
# and to ensure logs are flushed immediately (useful for monitoring)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies (required for some C-extensions in Pandas/SciPy)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source code
COPY src/ /app/src/