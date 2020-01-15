FROM golang:1.13.3-alpine

RUN apk add --update --no-cache ca-certificates make curl git mercurial bzr openssh protobuf python

# Enable 

RUN curl https://bootstrap.pypa.io/get-pip.py | python
RUN pip install PyYAML==3.13

RUN go get -u golang.org/x/lint/golint
RUN go get github.com/axw/gocov/gocov
RUN go get github.com/dmetzgar/goveralls
RUN go get golang.org/x/tools/cmd/cover

# Need this for downloading dependencies for private repos
RUN git config --global url."git@github.com:".insteadOf "https://github.com/"


# https://github.com/docker-library/golang/blob/c1baf037d71331eb0b8d4c70cff4c29cf124c5e0/1.4/Dockerfile
RUN mkdir -p /temporal
WORKDIR /temporal
