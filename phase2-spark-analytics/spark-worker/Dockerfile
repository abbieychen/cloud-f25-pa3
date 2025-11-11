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
RUN pip3 install --no-cache-dir \
    pymysql==1.0.2 \
    sqlalchemy==2.0.19 \
    mysql-connector-python==8.0.33

# Create spark directories
RUN mkdir -p /opt/spark/apps /opt/spark/logs /opt/spark/work

# Copy configuration and entrypoint
COPY worker_config.sh /opt/worker_config.sh
COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh /opt/worker_config.sh

# Expose ports
EXPOSE 8081

ENTRYPOINT [ "/opt/entrypoint.sh" ]
