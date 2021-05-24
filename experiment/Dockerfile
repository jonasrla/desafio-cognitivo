FROM bde2020/spark-base:3.1.1-hadoop3.2

RUN apk update && \
    apk upgrade

RUN pip3 install --upgrade pip && \
    pip3 install Faker

WORKDIR /app
COPY . .

ENTRYPOINT ["sh", "./experiment.sh"]