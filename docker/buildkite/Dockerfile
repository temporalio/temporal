FROM golang:1.12.1

# Tried to set Python to ignore warnings due to the instructions at this link:
# https://github.com/yaml/pyyaml/wiki/PyYAML-yaml.load(input)-Deprecation
# But this causes all the pip installs to fail, so don't do this:
# ENV PYTHONWARNINGS=ignore::yaml.YAMLLoadWarning
# ENV PYTHONWARNINGS=ignore

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      gettext-base \
      libyaml-dev \
      openjdk-8-jre \
      python \
      python-dev \
      python-pip \
      python-setuptools \
      time \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py | python
RUN pip install PyYAML==3.13

RUN go get -u github.com/golang/dep/cmd/dep
RUN go get -u github.com/Masterminds/glide
RUN go get -u golang.org/x/lint/golint
RUN go get github.com/axw/gocov/gocov
RUN go get github.com/mattn/goveralls
RUN go get golang.org/x/tools/cmd/cover

# The golang Docker sets the $GOPATH to be /go
# https://github.com/docker-library/golang/blob/c1baf037d71331eb0b8d4c70cff4c29cf124c5e0/1.4/Dockerfile
RUN mkdir -p /go/src/github.com/uber/cadence
WORKDIR /go/src/github.com/uber/cadence
