ARG TARGET=server
ARG GOPROXY

##### Temporal builder #####
FROM temporalio/base-builder:1.0.0 AS temporal-builder
WORKDIR /temporal

# Copy go.mod first to build docker layer with go dependencies (to improve rebuild time).
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 make bins

##### Temporal server #####
FROM temporalio/base-server:1.0.0 AS temporal-server
WORKDIR /etc/temporal
ENV TEMPORAL_HOME /etc/temporal
ENV SERVICES="history:matching:frontend:worker"
EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239
ENTRYPOINT ["/entrypoint.sh"]
CMD /start.sh

COPY docker/entrypoint.sh /entrypoint.sh
COPY config/dynamicconfig /etc/temporal/config/dynamicconfig
COPY docker/config_template.yaml /etc/temporal/config/config_template.yaml
COPY docker/start-temporal.sh /start-temporal.sh
COPY docker/start.sh /start.sh

COPY --from=temporal-builder /temporal/schema /etc/temporal/schema
COPY --from=temporal-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-sql-tool /usr/local/bin
COPY --from=temporal-builder /temporal/tctl /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-server /usr/local/bin

##### Auto setup Temporal server #####
FROM temporal-server AS temporal-auto-setup
CMD /start.sh autosetup

##### Temporal CLI (tctl) #####
FROM temporalio/base-server:1.0.0 AS temporal-tctl
WORKDIR /etc/temporal
ENTRYPOINT ["tctl"]
COPY --from=temporal-builder /temporal/tctl /usr/local/bin

##### Temporal admin tools #####
FROM temporalio/base-admin-tools:1.0.0 as temporal-admin-tools
WORKDIR /etc/temporal
# Keep the container running.
ENTRYPOINT ["tail", "-f", "/dev/null"]

COPY --from=temporal-builder /temporal/schema /etc/temporal/schema
COPY --from=temporal-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-sql-tool /usr/local/bin
COPY --from=temporal-builder /temporal/tctl /usr/local/bin


##### Build requested image #####
FROM temporal-${TARGET}
