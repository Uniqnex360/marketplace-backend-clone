FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        build-essential \
        libpq-dev \
        python3-dev \
        pkg-config \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install build dependencies first
RUN pip install --upgrade pip setuptools wheel

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application
COPY . .

CMD ["gunicorn", "ecommerce_tool.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3", "--timeout", "60"]
