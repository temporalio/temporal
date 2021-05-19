module go.temporal.io/server

go 1.16

require (
	cloud.google.com/go/storage v1.15.0
	// v0.0.0-20161221203622-b2a4d4ae21c7 is important here.
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/aws/aws-sdk-go v1.38.26
	github.com/benbjohnson/clock v1.1.0 // indirect
	github.com/bitly/go-hostpool v0.1.0 // indirect
	github.com/blang/semver/v4 v4.0.0
	github.com/cactus/go-statsd-client/statsd v0.0.0-20200423205355-cb0885a1018c
	github.com/cch123/elasticsql v1.0.1
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/emirpasic/gods v0.0.0-20190624094223-e689965507ab
	github.com/fatih/color v1.10.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20210425135552-909f2a77f46e
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.5.0
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/uuid v1.2.0
	github.com/gorilla/websocket v1.4.2
	github.com/hashicorp/go-hclog v0.14.1
	github.com/hashicorp/go-plugin v1.4.0
	github.com/iancoleman/strcase v0.1.3
	github.com/jmoiron/sqlx v1.3.4
	github.com/jonboulle/clockwork v0.2.2
	github.com/kr/pretty v0.2.1 // indirect
	github.com/lib/pq v1.10.1
	github.com/m3db/prometheus_client_golang v0.8.1
	github.com/m3db/prometheus_client_model v0.1.0 // indirect
	github.com/m3db/prometheus_common v0.1.0 // indirect
	github.com/m3db/prometheus_procfs v0.8.1 // indirect
	github.com/mattn/go-runewidth v0.0.12 // indirect
	github.com/mattn/go-sqlite3 v1.14.7 // indirect
	github.com/olekukonko/tablewriter v0.0.5
	github.com/olivere/elastic v6.2.35+incompatible
	github.com/olivere/elastic/v7 v7.0.24
	github.com/pborman/uuid v1.2.1
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/robfig/cron v1.2.0
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/temporalio/ringpop-go v0.0.0-20200708034907-1e016ebb537a
	github.com/uber-common/bark v1.3.0 // indirect
	github.com/uber-go/tally v3.3.17+incompatible
	github.com/uber/tchannel-go v1.21.1
	github.com/urfave/cli v1.22.5
	github.com/urfave/cli/v2 v2.3.0
	github.com/valyala/fastjson v1.6.3
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.opentelemetry.io/otel v0.15.0
	go.opentelemetry.io/otel/exporters/metric/prometheus v0.15.0
	go.temporal.io/api v1.4.1-0.20210429002340-277727e03039
	go.temporal.io/sdk v1.6.0
	go.temporal.io/version v0.0.0-20201015012359-4d3bb966d193
	go.uber.org/atomic v1.7.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20210421170649-83a5a9bb288b // indirect
	golang.org/x/mod v0.4.2 // indirect
	golang.org/x/oauth2 v0.0.0-20210413134643-5e61552d6c78
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/api v0.45.0
	google.golang.org/grpc v1.37.0
	google.golang.org/grpc/examples v0.0.0-20210426212906-52a707c0dafe
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gopkg.in/square/go-jose.v2 v2.5.1
	gopkg.in/validator.v2 v2.0.0-20210331031555-b37d688a7fb0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	honnef.co/go/tools v0.1.0 // indirect
)
