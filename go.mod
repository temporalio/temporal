module github.com/temporalio/temporal

go 1.12

require (
	github.com/DataDog/zstd v1.4.0 // indirect
	github.com/Shopify/sarama v1.23.0
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7
	github.com/benbjohnson/clock v1.0.0 // indirect
	github.com/bitly/go-hostpool v0.0.0-20171023180738-a3a6125de932 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bsm/sarama-cluster v2.1.13+incompatible
	github.com/cactus/go-statsd-client v3.1.1+incompatible
	github.com/cch123/elasticsql v0.0.0-20190321073543-a1a440758eb9
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/emirpasic/gods v0.0.0-20190624094223-e689965507ab
	github.com/fatih/color v0.0.0-20181010231311-3f9d52f7176a
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20171220143535-56a164ee9f31
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.1.0
	github.com/golang/mock v1.3.1
	github.com/google/uuid v1.1.1
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/go-version v1.2.0
	github.com/iancoleman/strcase v0.0.0-20190422225806-e506e3ef7365
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/jonboulle/clockwork v0.1.0
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/lib/pq v1.2.0
	github.com/m3db/prometheus_client_golang v0.8.1
	github.com/m3db/prometheus_client_model v0.1.0 // indirect
	github.com/m3db/prometheus_common v0.1.0 // indirect
	github.com/m3db/prometheus_procfs v0.8.1 // indirect
	github.com/mailru/easyjson v0.7.0 // indirect
	github.com/mattn/go-colorable v0.1.4 // indirect
	github.com/mattn/go-isatty v0.0.10 // indirect
	github.com/mattn/go-runewidth v0.0.6 // indirect
	github.com/mattn/go-sqlite3 v1.11.0 // indirect
	github.com/olekukonko/tablewriter v0.0.1
	github.com/olivere/elastic v6.2.21+incompatible
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v0.0.0-20180906182336-adf5a7427709
	github.com/pierrec/lz4 v0.0.0-20190701081048-057d66e894a4 // indirect
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/uber-common/bark v1.2.1 // indirect
	github.com/uber-go/kafka-client v0.2.3-0.20191018205945-8b3555b395f9
	github.com/uber-go/tally v3.3.13+incompatible
	github.com/uber/ringpop-go v0.8.5
	github.com/uber/tchannel-go v1.16.0
	github.com/urfave/cli v1.20.0
	github.com/valyala/fastjson v1.4.1
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.temporal.io/temporal v0.10.3
	go.temporal.io/temporal-proto v0.0.0-20200121224408-c93f8f990745
	go.uber.org/atomic v1.5.0
	go.uber.org/config v1.3.1
	go.uber.org/multierr v1.4.0
	go.uber.org/thriftrw v1.20.2
	go.uber.org/yarpc v1.42.1
	go.uber.org/zap v1.13.0
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4 // indirect
	golang.org/x/net v0.0.0-20191101175033-0deb6923b6d9
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20191114200427-caa0b0f7d508
	google.golang.org/appengine v1.6.1 // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
	gopkg.in/validator.v2 v2.0.0-20180514200540-135c24b11c19
	gopkg.in/yaml.v2 v2.2.4
)

// TODO https://github.com/uber/cadence/issues/2863
replace github.com/jmoiron/sqlx v1.2.0 => github.com/longquanzheng/sqlx v0.0.0-20191125235044-053e6130695c
