module go.temporal.io/server

go 1.21

require (
	cloud.google.com/go/storage v1.36.0
	github.com/aws/aws-sdk-go v1.44.289
	github.com/blang/semver/v4 v4.0.0
	github.com/cactus/go-statsd-client/v5 v5.0.0
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/emirpasic/gods v1.18.1
	github.com/fatih/color v1.15.0
	github.com/go-faker/faker/v4 v4.2.0
	github.com/go-sql-driver/mysql v1.7.1
	github.com/gocql/gocql v1.5.2
	github.com/golang-jwt/jwt/v4 v4.5.0
	github.com/golang/mock v1.7.0-rc.1
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.0
	github.com/iancoleman/strcase v0.3.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/lib/pq v1.10.9
	github.com/nexus-rpc/sdk-go v0.0.7
	github.com/olekukonko/tablewriter v0.0.5
	github.com/olivere/elastic/v7 v7.0.32
	github.com/pborman/uuid v1.2.1
	github.com/prometheus/client_golang v1.16.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/sony/gobreaker v0.5.0
	github.com/stretchr/testify v1.9.0
	github.com/temporalio/ringpop-go v0.0.0-20230606200434-b5c079f412d3
	github.com/temporalio/sqlparser v0.0.0-20231115171017-f4060bcfa6cb
	github.com/temporalio/tchannel-go v1.22.1-0.20231116015023-bd4fb7678499
	github.com/temporalio/tctl-kit v0.0.0-20230328153839-577f95d16fa0
	github.com/uber-go/tally/v4 v4.1.7
	github.com/urfave/cli v1.22.14
	github.com/urfave/cli/v2 v2.4.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.47.0
	go.opentelemetry.io/otel v1.22.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.42.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.19.0
	go.opentelemetry.io/otel/exporters/prometheus v0.42.0
	go.opentelemetry.io/otel/metric v1.22.0
	go.opentelemetry.io/otel/sdk v1.21.0
	go.opentelemetry.io/otel/sdk/metric v1.19.0
	go.temporal.io/api v1.32.1-0.20240423142804-7e067f47ee05
	go.temporal.io/sdk v1.26.1
	go.temporal.io/version v0.3.0
	go.uber.org/atomic v1.11.0
	go.uber.org/automaxprocs v1.5.2
	go.uber.org/fx v1.20.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.26.0
	golang.org/x/exp v0.0.0-20231127185646-65229373498e
	golang.org/x/oauth2 v0.17.0
	golang.org/x/sync v0.6.0
	golang.org/x/time v0.5.0
	google.golang.org/api v0.162.0
	google.golang.org/grpc v1.63.2
	google.golang.org/grpc/examples v0.0.0-20230623203957-0b3a81eabc28
	gopkg.in/square/go-jose.v2 v2.6.0
	gopkg.in/validator.v2 v2.0.1
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/sqlite v1.23.1
)

require (
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.47.0 // indirect
)

require (
	cloud.google.com/go v0.112.0 // indirect
	cloud.google.com/go/compute v1.24.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.6 // indirect
	github.com/apache/thrift v0.18.1 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-hostpool v0.1.0 // indirect
	github.com/cactus/go-statsd-client/statsd v0.0.0-20200423205355-cb0885a1018c // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.19.1
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/jackc/pgx/v5 v5.5.4
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.4.0
	github.com/prometheus/common v0.44.0
	github.com/prometheus/procfs v0.11.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/uber-common/bark v1.3.0 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.42.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.19.0 // indirect
	go.opentelemetry.io/otel/trace v1.22.0
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	go.uber.org/dig v1.17.0 // indirect
	golang.org/x/crypto v0.22.0 // indirect
	golang.org/x/mod v0.14.0 // indirect
	golang.org/x/net v0.24.0 // indirect
	golang.org/x/sys v0.19.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/tools v0.16.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240412170617-26222e5d3d56
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240412170617-26222e5d3d56 // indirect
	google.golang.org/protobuf v1.33.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	lukechampine.com/uint128 v1.3.0 // indirect
	modernc.org/cc/v3 v3.41.0 // indirect
	modernc.org/ccgo/v3 v3.16.14 // indirect
	modernc.org/libc v1.24.1 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.6.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.1.0 // indirect
)
