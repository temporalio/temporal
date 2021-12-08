ARG BASE_BUILDER_IMAGE=temporalio/base-builder:1.5.0
ARG BASE_SERVER_IMAGE=temporalio/base-server:1.4.0
ARG BASE_ADMIN_TOOLS_IMAGE=temporalio/base-admin-tools:1.3.0
ARG GOPROXY

##### Temporal builder #####
FROM ${BASE_BUILDER_IMAGE} AS temporal-builder

WORKDIR /temporal

# Copy go.mod/go.sum first to build docker layer with go dependencies (to improve rebuild time).
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make bins

##### Temporal server #####
FROM ${BASE_SERVER_IMAGE} AS temporal-server
WORKDIR /etc/temporal
ENV TEMPORAL_HOME /etc/temporal
ENV SERVICES "history:matching:frontend:worker"
EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239

# TODO (alex): switch WORKDIR to /home/temporal and remove "mkdir" and "chown" calls.
RUN addgroup -g 1000 temporal
RUN adduser -u 1000 -G temporal -D temporal
RUN mkdir /etc/temporal/config
RUN chown -R temporal:temporal /etc/temporal/config

USER temporal
ENTRYPOINT ["/etc/temporal/entrypoint.sh"]

COPY config/dynamicconfig /etc/temporal/config/dynamicconfig
COPY docker/config_template.yaml /etc/temporal/config/config_template.yaml
COPY docker/entrypoint.sh /etc/temporal/entrypoint.sh
COPY docker/start-temporal.sh /etc/temporal/start-temporal.sh

COPY --from=temporal-builder /temporal/tctl /usr/local/bin
COPY --from=temporal-builder /temporal/tctl-authorization-plugin /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-server /usr/local/bin

##### Auto setup Temporal server #####
FROM temporal-server AS temporal-auto-setup
CMD ["autosetup"]

COPY docker/auto-setup.sh /etc/temporal/auto-setup.sh

COPY --from=temporal-builder /temporal/schema /etc/temporal/schema
COPY --from=temporal-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-sql-tool /usr/local/bin

##### Development configuration for Temporal with additional set of tools #####
FROM temporal-auto-setup as temporal-develop

# apk and setup-develop.sh requires root permissions.
USER root
# iproute2 contains tc, which can be used for traffic shaping in resiliancy testing. 
ONBUILD RUN apk add iproute2

CMD ["autosetup", "develop"]

COPY docker/setup-develop.sh /etc/temporal/setup-develop.sh

##### Temporal CLI (tctl) #####
FROM ${BASE_SERVER_IMAGE} AS temporal-tctl
WORKDIR /etc/temporal
ENTRYPOINT ["tctl"]
COPY --from=temporal-builder /temporal/tctl /usr/local/bin
COPY --from=temporal-builder /temporal/tctl-authorization-plugin /usr/local/bin

##### Temporal admin tools #####
FROM ${BASE_ADMIN_TOOLS_IMAGE} as temporal-admin-tools
WORKDIR /etc/temporal
# Keep the container running.
ENTRYPOINT ["tail", "-f", "/dev/null"]

COPY --from=temporal-builder /temporal/schema /etc/temporal/schema
COPY --from=temporal-builder /temporal/temporal-cassandra-tool /usr/local/bin
COPY --from=temporal-builder /temporal/temporal-sql-tool /usr/local/bin
COPY --from=temporal-builder /temporal/tctl /usr/local/bin
COPY --from=temporal-builder /temporal/tctl-authorization-plugin /usr/local/bin
