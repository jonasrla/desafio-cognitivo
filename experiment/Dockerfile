FROM bde2020/spark-base:3.1.1-hadoop3.2

RUN pip3 install --upgrade pip
RUN pip3 install Faker

WORKDIR /app
COPY setup.py .
COPY reader.py .
COPY experiment.sh .

ENTRYPOINT ["sh", "./experiment.sh"]