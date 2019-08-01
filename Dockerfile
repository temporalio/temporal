ARG TARGET=server

# Build Cadence binaries
FROM golang:1.12.7-alpine AS builder

RUN apk add --update --no-cache ca-certificates make git curl mercurial bzr

WORKDIR /go/src/github.com/uber/cadence

RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | INSTALL_DIRECTORY=/usr/local/bin sh
COPY Gopkg.* ./
RUN dep ensure -v -vendor-only

COPY . .
RUN sed -i 's/dep-ensured//g' Makefile
RUN CGO_ENABLED=0 make copyright cadence-cassandra-tool cadence-sql-tool cadence cadence-server


# Download dockerize
FROM alpine:3.10 AS dockerize

RUN apk add --no-cache openssl

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz


# Alpine base image
FROM alpine:3.10 AS alpine

RUN apk add --update --no-cache ca-certificates tzdata bash curl

# set up nsswitch.conf for Go's "netgo" implementation
# https://github.com/gliderlabs/docker-alpine/issues/367#issuecomment-424546457
RUN test ! -e /etc/nsswitch.conf && echo 'hosts: files dns' > /etc/nsswitch.conf

SHELL ["/bin/bash", "-c"]


# Cadence server
FROM alpine AS cadence-server

ENV CADENCE_HOME /etc/cadence
RUN mkdir -p /etc/cadence

COPY --from=dockerize /usr/local/bin/dockerize /usr/local/bin
COPY --from=builder /go/src/github.com/uber/cadence/cadence-cassandra-tool /usr/local/bin
COPY --from=builder /go/src/github.com/uber/cadence/cadence-sql-tool /usr/local/bin
COPY --from=builder /go/src/github.com/uber/cadence/cadence /usr/local/bin
COPY --from=builder /go/src/github.com/uber/cadence/cadence-server /usr/local/bin
COPY --from=builder /go/src/github.com/uber/cadence/schema /etc/cadence/schema

COPY docker/entrypoint.sh /docker-entrypoint.sh
COPY config/dynamicconfig /etc/cadence/config/dynamicconfig
COPY docker/config_template.yaml /etc/cadence/config

WORKDIR /etc/cadence

ENV SERVICES="history,matching,frontend,worker"

EXPOSE 7933 7934 7935 7939
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD dockerize -template /etc/cadence/config/config_template.yaml:/etc/cadence/config/docker.yaml cadence-server --root $CADENCE_HOME --env docker start --services=$SERVICES


# All-in-one Cadence server
FROM cadence-server AS cadence-allinone

RUN apk add --update --no-cache ca-certificates py-pip mysql-client
RUN pip install cqlsh

COPY docker/start.sh /start.sh

CMD /start.sh


# Cadence CLI
FROM alpine AS cadence-cli

COPY --from=builder /go/src/github.com/uber/cadence/cadence /usr/local/bin

ENTRYPOINT ["cadence"]


# Final image
FROM cadence-${TARGET}
