ARG BASE_SERVER_IMAGE=temporalio/base-server:1.15.12

FROM ${BASE_SERVER_IMAGE} as temporal-server
ARG TARGETARCH
ARG TEMPORAL_SHA=unknown

WORKDIR /etc/temporal

ENV TEMPORAL_HOME=/etc/temporal
EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239

RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal && \
    mkdir /etc/temporal/config && \
    chown -R temporal:temporal /etc/temporal/config

USER temporal

ENV TEMPORAL_SHA=${TEMPORAL_SHA}

# Binaries from goreleaser + CLI download
COPY ./.docker/dist/${TARGETARCH}/temporal-server /usr/local/bin/
COPY ./.docker/dist/${TARGETARCH}/temporal /usr/local/bin/

# Configs
COPY ./config/dynamicconfig/docker.yaml /etc/temporal/config/dynamicconfig/docker.yaml
COPY ./docker/config_template.yaml /etc/temporal/config/config_template.yaml

# Scripts
COPY ./.docker/scripts/entrypoint.sh /etc/temporal/entrypoint.sh
COPY ./.docker/scripts/start-temporal.sh /etc/temporal/start-temporal.sh

ENTRYPOINT ["/etc/temporal/entrypoint.sh"]
