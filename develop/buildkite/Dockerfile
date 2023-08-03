FROM temporalio/base-ci-builder:1.10.3
WORKDIR /temporal

# Populate some data in the go build cache to speed up subsequent builds.
# Deliberately spread it into multiple layers to reduce data that has to be
# pushed. After each step, set mtimes on the build cache to a fixed value so
# that mtime won't cause layer differences.

ADD go.mod go.sum /temporal/

# Download all deps. This layer should be shared between most builds.
RUN go mod download && find /go -print0 | xargs -0 touch -d 2000-01-01

# Build some third-party deps of functional tests. This layer should be shared between most builds.
# (This is kind of gross, but spreads out layers more for faster uploads.)
RUN go build github.com/gocql/gocql github.com/jmoiron/sqlx github.com/lib/pq github.com/temporalio/ringpop-go github.com/temporalio/tchannel-go github.com/uber-go/tally/v4 github.com/xwb1989/sqlparser go.opentelemetry.io/otel go.uber.org/fx google.golang.org/grpc gopkg.in/yaml.v3 modernc.org/sqlite && find /go -print0 | xargs -0 touch -d 2000-01-01

ADD . /temporal

# Build rest of third-party deps of functional tests. This layer should be shared between most builds.
RUN go list -f $'{{range .Deps}}{{.}}\n{{end}}' ./tests | grep '\.' | grep -v go.temporal.io/server | xargs go build && find /go -print0 | xargs -0 touch -d 2000-01-01

# Build functional tests with race and coverage. xdc and ndc are smaller so
# don't worry about pre-building them. This layer will probably not be shared
# but will speed up jobs within this build.
RUN make pre-build-functional-test-coverage && find /go -print0 | xargs -0 touch -d 2000-01-01
