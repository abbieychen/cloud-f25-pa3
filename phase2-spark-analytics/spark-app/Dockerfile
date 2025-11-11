FROM apache/spark:3.4.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /opt/spark/apps/
WORKDIR /opt/spark/apps

# Create logs directory
RUN mkdir -p /opt/spark/logs

RUN chmod +x /opt/spark/apps/run_app.sh

CMD [ "/opt/spark/apps/run_app.sh" ]
