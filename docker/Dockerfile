# docker/Dockerfile

FROM python:3.9-slim

# Install system dependencies (jika diperlukan untuk library tertentu)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file dari folder luar ke dalam container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command default agar container tetap menyala
CMD ["tail", "-f", "/dev/null"]

COPY src/ ./src
