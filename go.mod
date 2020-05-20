module github.com/temporalio/temporal

go 1.14

require (
	cloud.google.com/go/storage v1.8.0
	github.com/Shopify/sarama v1.26.4
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/aws/aws-sdk-go v1.31.1
	github.com/benbjohnson/clock v1.0.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bitly/go-hostpool v0.1.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b // indirect
	github.com/bsm/sarama-cluster v2.1.15+incompatible
	github.com/cactus/go-statsd-client/statsd v0.0.0-20200423205355-cb0885a1018c
	github.com/cch123/elasticsql v1.0.1
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/emirpasic/gods v0.0.0-20190624094223-e689965507ab
	github.com/fatih/color v1.9.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20171220143535-56a164ee9f31
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.1.0
	github.com/golang/mock v1.4.3
	github.com/golang/protobuf v1.4.2
	github.com/google/uuid v1.1.1
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/go-version v1.2.0
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334
	github.com/jmoiron/sqlx v1.2.0
	github.com/jonboulle/clockwork v0.1.0
	github.com/klauspost/compress v1.10.5 // indirect
	github.com/lib/pq v1.5.2
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
	github.com/olivere/elastic v6.2.31+incompatible
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v1.2.0
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/prashantv/protectmem v0.0.0-20171002184600-e20412882b3a // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.6.0
	github.com/streadway/quantile v0.0.0-20150917103942-b0c588724d25 // indirect
	github.com/stretchr/testify v1.5.1
	github.com/uber-common/bark v1.3.0 // indirect
	github.com/uber-go/kafka-client v0.2.3-0.20191018205945-8b3555b395f9
	github.com/uber-go/tally v3.3.16+incompatible
	github.com/uber/ringpop-go v0.8.5
	github.com/uber/tchannel-go v1.18.0
	github.com/urfave/cli v1.22.4
	github.com/valyala/fastjson v1.5.1
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.temporal.io/temporal v0.23.1
	go.temporal.io/temporal-proto v0.23.1
	go.uber.org/atomic v1.6.0
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200510223506-06a226fb4e37 // indirect
	golang.org/x/mod v0.3.0 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1
	golang.org/x/tools v0.0.0-20200519205726-57a9e4404bf7 // indirect
	google.golang.org/api v0.24.0
	google.golang.org/grpc v1.29.1
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/validator.v2 v2.0.0-20191107172027-c3144fdedc21
	gopkg.in/yaml.v2 v2.3.0
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
)

// TODO https://github.com/uber/cadence/issues/2863
replace github.com/jmoiron/sqlx v1.2.0 => github.com/longquanzheng/sqlx v0.0.0-20191125235044-053e6130695c

replace go.temporal.io/temporal-proto v0.20.31 => ../temporal-proto-go

replace go.temporal.io/temporal v0.22.7 => ../temporal-go-sdk
