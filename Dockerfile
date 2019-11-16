ARG TARGET=server

# Can be used in case a proxy is necessary
ARG GOPROXY

# Build tcheck binary
FROM golang:1.13.3-alpine AS tcheck

RUN apk add --update --no-cache ca-certificates git curl

ENV GO111MODULE=off

RUN curl https://glide.sh/get | sh

ENV TCHECK_VERSION=v1.1.0

RUN go get -d github.com/uber/tcheck
RUN cd /go/src/github.com/uber/tcheck && git checkout ${TCHECK_VERSION}

WORKDIR /go/src/github.com/uber/tcheck

RUN glide install

RUN go install

# Build Cadence binaries
FROM golang:1.13.3-alpine AS builder

RUN apk add --update --no-cache ca-certificates make curl git mercurial bzr openssh protobuf

# add credentials to builder
ARG SSH_PRIVATE_KEY
RUN mkdir /root/.ssh/
RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa

RUN chmod 600 /root/.ssh/id_rsa

# make sure domain is accepted
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"

# Making sure that dependency is not touched
ENV GOFLAGS="-mod=readonly"

WORKDIR /temporal

# Copy go mod dependencies and build cache
COPY go.* ./
#COPY .gen/proto/go.* ./.gen/proto/
COPY . .

RUN go mod download



# need to make clean first in case binaries to be built are stale
RUN make clean && CGO_ENABLED=0 make copyright cadence-cassandra-tool cadence-sql-tool cadence cadence-server

# Download dockerize
FROM alpine:3.10 AS dockerize

RUN apk add --no-cache openssl

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && echo "**** fix for host id mapping error ****" \
    && chown root:root /usr/local/bin/dockerize

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

COPY --from=tcheck /go/bin/tcheck /usr/local/bin
COPY --from=dockerize /usr/local/bin/dockerize /usr/local/bin
COPY --from=builder /temporal/cadence-cassandra-tool /usr/local/bin
COPY --from=builder /temporal/cadence-sql-tool /usr/local/bin
COPY --from=builder /temporal/cadence /usr/local/bin
COPY --from=builder /temporal/cadence-server /usr/local/bin
COPY --from=builder /temporal/schema /etc/cadence/schema

COPY docker/entrypoint.sh /docker-entrypoint.sh
COPY config/dynamicconfig /etc/cadence/config/dynamicconfig
COPY docker/config_template.yaml /etc/cadence/config
COPY docker/start-cadence.sh /start-cadence.sh

WORKDIR /etc/cadence

ENV SERVICES="history,matching,frontend,worker"

EXPOSE 7933 7934 7935 7939
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD /start-cadence.sh


# All-in-one Cadence server
FROM cadence-server AS cadence-auto-setup

RUN apk add --update --no-cache ca-certificates py-pip mysql-client
RUN pip install cqlsh

COPY docker/start.sh /start.sh

CMD /start.sh

# Cadence CLI
FROM alpine AS cadence-cli

COPY --from=tcheck /go/bin/tcheck /usr/local/bin
COPY --from=builder /temporal/cadence /usr/local/bin

ENTRYPOINT ["cadence"]


# Final image
FROM cadence-${TARGET}
