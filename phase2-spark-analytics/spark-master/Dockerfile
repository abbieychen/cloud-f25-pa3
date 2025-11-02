FROM apache/spark:3.4.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    build-essential \
    curl \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip3 install --no-cache-dir \
    pymysql==1.0.2 \
    sqlalchemy==2.0.19 \
    matplotlib==3.7.1 \
    seaborn==0.12.2 \
    pandas==2.0.3 \
    numpy==1.24.3 \
    scipy==1.10.1 \
    mysql-connector-python==8.0.33

# Create spark directories
RUN mkdir -p /opt/spark/apps /opt/spark/logs /opt/spark/work

# Copy configuration and entrypoint
COPY spark-defaults.conf /opt/spark/conf/
COPY log4j.properties /opt/spark/conf/
COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

# Expose ports
EXPOSE 7077 8080 6066

ENTRYPOINT [ "/opt/entrypoint.sh" ]
