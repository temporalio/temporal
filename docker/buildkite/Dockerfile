FROM golang:1.13.3-alpine AS builder

RUN apk add --update --no-cache ca-certificates make curl git mercurial bzr openssh protobuf

# add credentials to builder
# ARG SSH_PRIVATE_KEY
# RUN mkdir /root/.ssh/
# RUN echo "${SSH_PRIVATE_KEY}" > /root/.ssh/id_rsa

# RUN chmod 600 /root/.ssh/id_rsa

# make sure domain is accepted
# RUN touch /root/.ssh/known_hosts
# RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"

# Making sure that dependency is not touched
ENV GOFLAGS="-mod=readonly"

WORKDIR /temporal

# Copy go mod dependencies and build cache
COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . .