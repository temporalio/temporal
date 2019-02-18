# Copyright (c) 2017 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

# Build cadence binaries
FROM golang:1.11.1 as builder

# Specify the cadence version to be built into this docker image
# See https://github.com/uber/cadence/tags
ARG git_branch
RUN if [ -z "$git_branch" ]; then echo "ERROR: git_branch NOT SET. Usage: docker build . --build-arg git_branch=YOUR_CHECKOUT_BRANCH"; exit 1; else : ; fi

RUN go get -u github.com/golang/dep/cmd/dep
RUN go get -u github.com/golang/lint/golint

# get and compile cadence-server
RUN git clone https://github.com/uber/cadence.git /go/src/github.com/uber/cadence
RUN cd /go/src/github.com/uber/cadence && git checkout $git_branch && make bins_nothrift


# Final Cadence image
FROM debian:jessie

RUN apt-get update && apt-get install -y --no-install-recommends \
		gettext-base \
		wget \
		vim \
		tcpdump \
		netcat \
		python-pip \
	&& rm -rf /var/lib/apt/lists/*

RUN pip install cqlsh

RUN mkdir /cadence
ENV CADENCE_HOME /cadence

EXPOSE 7933 7934 7935 7939

COPY ./start.sh $CADENCE_HOME/start.sh
COPY ./config_template.yaml $CADENCE_HOME/config/docker_template.yaml
RUN chmod a+x $CADENCE_HOME/start.sh

COPY --from=builder /go/src/github.com/uber/cadence/cadence-cassandra-tool /cadence
COPY --from=builder /go/src/github.com/uber/cadence/cadence /cadence
COPY --from=builder /go/src/github.com/uber/cadence/cadence-server /cadence
COPY --from=builder /go/src/github.com/uber/cadence/schema/ /cadence/schema/

WORKDIR $CADENCE_HOME
CMD ./start.sh $CADENCE_HOME
