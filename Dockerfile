FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . /app
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

CMD ["streamlit", "run", "crawl.py"]
