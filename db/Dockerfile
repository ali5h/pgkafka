# build wal2json
FROM postgres:11.1 as builder

RUN set -ex; \
    apt-get update; \
    apt-get install  -y --no-install-recommends \
    ca-certificates \
    gcc \
    make \
    curl \
    postgresql-server-dev-11 \
    ;\
    rm -rf /var/lib/apt/lists/*

RUN set -ex; \
    commit=92b33c7d7c2fccbeb9f79455dafbc92e87e00ddd ;\
    (curl -Ls "https://github.com/eulerto/wal2json/archive/${commit}.tar.gz" | tar xz); \
    cd "wal2json-${commit}"; USE_PGXS=1 make; \
    cp wal2json.so /; \
    cd ..; rm -rf "wal2json-${commit}"

# db image with wal2json installed
FROM postgres:11.1 as db
RUN mkdir -p '/usr/lib/postgresql/11/lib'
COPY --from=builder /wal2json.so /usr/lib/postgresql/11/lib/
