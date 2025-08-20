# Use official Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (clean cache in one layer)
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY ecommerce_tool/requirements.txt .

# Upgrade pip and install Python dependencies
RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# Copy project files
COPY . .

# Set environment variables
ENV DJANGO_SETTINGS_MODULE=ecommerce_tool.ecommerce_tool.settings
ENV PYTHONUNBUFFERED=1

# Expose the port Render uses
EXPOSE 10000

# Start Gunicorn
CMD ["gunicorn", "ecommerce_tool.ecommerce_tool.wsgi:application", "--bind", "0.0.0.0:10000", "--workers", "3", "--threads", "2", "--timeout", "120"]
