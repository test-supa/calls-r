# Use official slim Python base image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Make startup script executable
RUN chmod +x start_services.sh

# Expose port 8080 (Web Dashboard) and 8081 (Health Checks)
EXPOSE 8080 8081

# Command to run 24/7 background worker
CMD ["./start_services.sh"]
