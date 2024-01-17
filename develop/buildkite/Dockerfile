FROM temporalio/base-ci-builder:1.10.8 AS prebuild
WORKDIR /temporal

# Populate some data in the go build cache to speed up subsequent builds.

ADD go.mod go.sum develop/buildkite/third_party_deps.txt /temporal/
# Download deps (in several layers to parallelize push/pull).
RUN go mod download github.com/aws/aws-sdk-go
RUN go mod download google.golang.org/api
RUN go mod download modernc.org/sqlite
RUN go mod download

# Build deps (in two layers to parallelize push/pull).
RUN head -100 third_party_deps.txt | xargs go build -race
RUN tail +100 third_party_deps.txt | xargs go build -race

FROM prebuild AS build

ADD . /temporal

# Build functional tests with race and coverage (the same way most of the tests run them).
RUN make pre-build-functional-test-coverage
