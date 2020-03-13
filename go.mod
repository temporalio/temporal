module github.com/temporalio/temporal

go 1.13

require (
	cloud.google.com/go v0.38.0
	github.com/Shopify/sarama v1.23.0
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7
	github.com/aws/aws-sdk-go v1.29.4
	github.com/benbjohnson/clock v1.0.0 // indirect
	github.com/bitly/go-hostpool v0.1.0 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869
	github.com/bsm/sarama-cluster v2.1.13+incompatible
	github.com/cactus/go-statsd-client v3.1.1+incompatible
	github.com/cch123/elasticsql v0.0.0-20190321073543-a1a440758eb9
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/emirpasic/gods v0.0.0-20190624094223-e689965507ab
	github.com/fatih/color v0.0.0-20181010231311-3f9d52f7176a
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20171220143535-56a164ee9f31
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.1.0
	github.com/golang/mock v1.4.0
	github.com/google/uuid v1.1.1
	github.com/hashicorp/go-version v1.2.0
	github.com/iancoleman/strcase v0.0.0-20190422225806-e506e3ef7365
	github.com/jmoiron/sqlx v1.2.0
	github.com/jonboulle/clockwork v0.1.0
	github.com/lib/pq v1.2.0
	github.com/m3db/prometheus_client_golang v0.8.1
	github.com/mailru/easyjson v0.7.0 // indirect
	github.com/mattn/go-colorable v0.1.4 // indirect
	github.com/mattn/go-isatty v0.0.10 // indirect
	github.com/mattn/go-runewidth v0.0.6 // indirect
	github.com/olekukonko/tablewriter v0.0.1
	github.com/olivere/elastic v6.2.21+incompatible
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v0.0.0-20180906182336-adf5a7427709
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/uber-go/kafka-client v0.2.3-0.20191018205945-8b3555b395f9
	github.com/uber-go/tally v3.3.15+incompatible
	github.com/uber/cadence v0.11.0
	github.com/uber/ringpop-go v0.8.5
	github.com/uber/tchannel-go v1.16.0
	github.com/urfave/cli v1.20.0
	github.com/valyala/fastjson v1.4.1
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.temporal.io/temporal v0.10.11
	go.temporal.io/temporal-proto v0.0.0-20200311025042-d9d2e74191de
	go.uber.org/atomic v1.5.1
	go.uber.org/cadence v0.9.1-0.20191023030824-883f86358883
	go.uber.org/multierr v1.4.0
	go.uber.org/thriftrw v1.20.2
	go.uber.org/zap v1.13.0
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	golang.org/x/oauth2 v0.0.0-20190226205417-e64efc72b421
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20191114200427-caa0b0f7d508
	google.golang.org/api v0.4.0
	google.golang.org/grpc v1.27.1
	gopkg.in/validator.v2 v2.0.0-20180514200540-135c24b11c19
	gopkg.in/yaml.v2 v2.2.4
)

// TODO https://github.com/uber/cadence/issues/2863
replace github.com/jmoiron/sqlx v1.2.0 => github.com/longquanzheng/sqlx v0.0.0-20191125235044-053e6130695c
