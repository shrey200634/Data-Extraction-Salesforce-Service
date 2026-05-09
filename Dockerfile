FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run the service
EXPOSE 5711
CMD ["gunicorn", "--bind", "0.0.0.0:5711", "--workers", "2", "--timeout", "120", "app.main:create_app()"]