FROM apache/airflow:3.0.0

# Switch to root to install Java
USER root

# Install OpenJDK 17 (compatible with Spark 3.5)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Verify Java installation
RUN java -version

# Switch back to airflow user
USER airflow

# Install Python packages for weather pipeline
RUN pip install --no-cache-dir \
    requests \
    tenacity \
    python-dotenv \
    pyspark==3.5.0 \
    pandas \
    pyarrow \
    psycopg[binary]

# Set Spark environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3