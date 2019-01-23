FROM golang:1.11.4-stretch as pgkafka

WORKDIR /pgkafka
COPY . .

RUN set -ex; \
    go install -v ./...

ENTRYPOINT [ "pgkafka" ]
