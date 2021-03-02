ARG TARGET=server

# Can be used in case a proxy is necessary
ARG GOPROXY

# Temporal server
FROM temporaliotest/base-server AS temporal-server

ENV TEMPORAL_HOME /etc/temporal
RUN mkdir -p /etc/temporal

COPY --from=temporaliotest/base-server /usr/local/bin/dockerize /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/temporal-sql-tool /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/tctl /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/temporal-server /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/schema /etc/temporal/schema

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
FROM base-server AS temporal-tctl

COPY --from=base-builder /temporal/tctl /usr/local/bin

ENTRYPOINT ["tctl"]

# All temporal tool binaries

ENV TEMPORAL_HOME /etc/temporal
RUN mkdir -p /etc/temporal

COPY --from=temporaliotest/base-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/temporal-sql-tool /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/tctl /usr/local/bin
COPY --from=temporaliotest/base-builder /temporal/schema /etc/temporal/schema

WORKDIR /etc/temporal

# keep the container running
ENTRYPOINT ["tail", "-f", "/dev/null"]

# Final image
FROM temporal-${TARGET}
