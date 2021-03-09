ARG TARGET=server

# Can be used in case a proxy is necessary
ARG GOPROXY

FROM temporalio/base-builder:1.0.0 AS temporal-builder

WORKDIR /temporal

# Copy go mod dependencies first and build docker cache
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 make bins

# Temporal server
FROM temporalio/base-server:1.0.0 AS temporal-server

ENV TEMPORAL_HOME /etc/temporal
RUN mkdir -p /etc/temporal

COPY --from=temporal-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-sql-tool /usr/local/bin
COPY --from=temporal-builder /temporal/tctl /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-server /usr/local/bin
COPY --from=temporal-builder /temporal/schema /etc/temporal/schema

COPY docker/entrypoint.sh /entrypoint.sh
COPY config/dynamicconfig /etc/temporal/config/dynamicconfig
COPY docker/config_template.yaml /etc/temporal/config/config_template.yaml
COPY docker/start-temporal.sh /start-temporal.sh
COPY docker/start.sh /start.sh

WORKDIR /etc/temporal

ENV SERVICES="history:matching:frontend:worker"

EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239
ENTRYPOINT ["/entrypoint.sh"]
CMD /start.sh

# All-in-one Temporal server
FROM temporal-server AS temporal-auto-setup
CMD /start.sh autosetup

# Temporal CLI
FROM temporalio/base-server:1.0.0 AS temporal-tctl

COPY --from=temporal-builder /temporal/tctl /usr/local/bin

ENTRYPOINT ["tctl"]

# All temporal tool binaries
FROM temporalio/base-admin-tools:1.0.0 as temporal-admin-tools

ENV TEMPORAL_HOME /etc/temporal
RUN mkdir -p /etc/temporal

COPY --from=temporal-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-sql-tool /usr/local/bin
COPY --from=temporal-builder /temporal/tctl /usr/local/bin
COPY --from=temporal-builder /temporal/schema /etc/temporal/schema

WORKDIR /etc/temporal

# keep the container running
ENTRYPOINT ["tail", "-f", "/dev/null"]

# Final image
FROM temporal-${TARGET}
