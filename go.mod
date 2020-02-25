module github.com/uber/cadence

go 1.12

require (
	cloud.google.com/go v0.38.0
	github.com/DataDog/zstd v1.4.0 // indirect
	github.com/Shopify/sarama v1.23.0
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7
	github.com/aws/aws-sdk-go v1.25.34
	github.com/benbjohnson/clock v0.0.0-20161215174838-7dc76406b6d3 // indirect
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869
	github.com/bsm/sarama-cluster v2.1.13+incompatible
	github.com/cactus/go-statsd-client v3.1.1+incompatible
	github.com/cch123/elasticsql v0.0.0-20190321073543-a1a440758eb9
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/emirpasic/gods v0.0.0-20190624094223-e689965507ab
	github.com/fatih/color v0.0.0-20181010231311-3f9d52f7176a
	github.com/fatih/structtag v1.1.0 // indirect
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20191126110522-1982a06ad6b9
	github.com/golang/mock v1.3.1
	github.com/google/uuid v1.1.1
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
	github.com/mailru/easyjson v0.0.0-20190626092158-b2ccc519800e // indirect
	github.com/mattn/go-colorable v0.0.9 // indirect
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/mattn/go-runewidth v0.0.4 // indirect
	github.com/mattn/go-sqlite3 v1.11.0 // indirect
	github.com/olekukonko/tablewriter v0.0.1
	github.com/olivere/elastic v6.2.21+incompatible
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v0.0.0-20180906182336-adf5a7427709
	github.com/pierrec/lz4 v0.0.0-20190701081048-057d66e894a4 // indirect
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/uber-common/bark v1.2.1 // indirect
	github.com/uber-go/kafka-client v0.2.3-0.20191018205945-8b3555b395f9
	github.com/uber-go/tally v3.3.15+incompatible
	github.com/uber/ringpop-go v0.8.5
	github.com/uber/tchannel-go v1.16.0
	github.com/urfave/cli v1.20.0
	github.com/valyala/fastjson v1.4.1
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.opencensus.io v0.22.2 // indirect
	go.uber.org/atomic v1.5.1
	go.uber.org/cadence v0.9.1-0.20200128004345-b282629d5ba9
	go.uber.org/multierr v1.3.0
	go.uber.org/net/metrics v1.2.0 // indirect
	go.uber.org/thriftrw v1.20.2
	go.uber.org/yarpc v1.42.0
	go.uber.org/zap v1.12.0
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sys v0.0.0-20191029155521-f43be2a4598c // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools v0.0.0-20200127195909-ed30b9180dd3
	google.golang.org/api v0.14.0
	google.golang.org/appengine v1.6.1 // indirect
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
	gopkg.in/validator.v2 v2.0.0-20180514200540-135c24b11c19
	gopkg.in/yaml.v2 v2.2.2
)

// TODO https://github.com/uber/cadence/issues/2863
replace github.com/jmoiron/sqlx v1.2.0 => github.com/longquanzheng/sqlx v0.0.0-20191125235044-053e6130695c
