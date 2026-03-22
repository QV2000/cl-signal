FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p /data

# Environment variables
ENV CL_SIGNAL_DB_PATH=/data/cl_signal.ddb
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "main.py"]
