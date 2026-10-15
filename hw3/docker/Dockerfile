FROM postgres:14.18-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip curl \
    && pip3 install --break-system-packages \
        "patroni[etcd3]==4.0.6" \
        "psycopg2-binary==2.9.10" \
    && rm -rf /var/lib/apt/lists/*

USER postgres
ENTRYPOINT ["patroni"]
