FROM golang:1.9 as go-builder

RUN apt-get update && apt-get install -y git

# Install glide
WORKDIR $GOPATH/src/github.com/Masterminds
RUN git clone https://github.com/Masterminds/glide.git
RUN cd glide && git checkout v0.12.3 && make bootstrap-dist && make install

RUN mkdir -p $GOPATH/src/github.com/uber/cadence
COPY . $GOPATH/src/github.com/uber/cadence
WORKDIR $GOPATH/src/github.com/uber/cadence
RUN make cadence

FROM phusion/baseimage:0.10.1

COPY --from=go-builder /go/src/github.com/uber/cadence/cadence .
COPY --from=go-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["./cadence"]

