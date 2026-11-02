FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
RUN (python3 -m pip --version > /dev/null 2>&1 || (apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*)) \
    && python3 -m pip install --no-cache-dir pymongo==4.8.0 numpy
USER spark
WORKDIR /app
