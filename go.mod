module go.temporal.io/server

go 1.14

require (
	cloud.google.com/go/storage v1.9.0
	github.com/Shopify/sarama v1.26.4
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/aws/aws-sdk-go v1.31.12
	github.com/benbjohnson/clock v1.0.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-hostpool v0.1.0 // indirect
	github.com/blang/semver/v4 v4.0.0
	github.com/bsm/sarama-cluster v2.1.15+incompatible
	github.com/cactus/go-statsd-client/statsd v0.0.0-20200423205355-cb0885a1018c
	github.com/cch123/elasticsql v1.0.1
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/emirpasic/gods v0.0.0-20190624094223-e689965507ab
	github.com/fatih/color v1.9.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20200624222514-34081eda590e
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.1.0
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.4.2
	github.com/google/uuid v1.1.2
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/jcmturner/gokrb5/v8 v8.3.0 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/jonboulle/clockwork v0.1.0
	github.com/klauspost/compress v1.10.8 // indirect
	github.com/lib/pq v1.6.0
	github.com/m3db/prometheus_client_golang v0.8.1
	github.com/m3db/prometheus_client_model v0.1.0 // indirect
	github.com/m3db/prometheus_common v0.1.0 // indirect
	github.com/m3db/prometheus_procfs v0.8.1 // indirect
	github.com/mailru/easyjson v0.7.1 // indirect
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/mattn/go-sqlite3 v2.0.3+incompatible // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/olekukonko/tablewriter v0.0.4
	github.com/olivere/elastic v6.2.34+incompatible
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/pborman/uuid v1.2.0
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.6.1
	github.com/temporalio/ringpop-go v0.0.0-20200708034907-1e016ebb537a
	github.com/uber-common/bark v1.3.0 // indirect
	github.com/uber-go/kafka-client v0.2.3-0.20191018205945-8b3555b395f9
	github.com/uber-go/tally v3.3.17+incompatible
	github.com/uber/tchannel-go v1.19.0
	github.com/urfave/cli v1.22.4
	github.com/valyala/fastjson v1.5.1
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.temporal.io/api v1.1.0
	go.temporal.io/sdk v1.1.0
	go.temporal.io/version v0.0.0-20201010013230-09a97e02aa8c
	go.uber.org/atomic v1.6.0
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.15.0
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	golang.org/x/tools v0.0.0-20200609164405-eb789aa7ce50 // indirect
	google.golang.org/api v0.26.0
	google.golang.org/grpc v1.32.0
	google.golang.org/grpc/examples v0.0.0-20200625174016-7a808837ae92
	gopkg.in/validator.v2 v2.0.0-20200605151824-2b28d334fa05
	gopkg.in/yaml.v2 v2.3.0
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776 // indirect
)

// TODO https://github.com/uber/cadence/issues/2863
replace github.com/jmoiron/sqlx v1.2.0 => github.com/longquanzheng/sqlx v0.0.0-20191125235044-053e6130695c
