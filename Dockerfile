FROM bde2020/spark-base:3.1.1-hadoop3.2

WORKDIR /app
COPY main.py .

ENTRYPOINT ["/spark/bin/spark-submit", "./main.py"]