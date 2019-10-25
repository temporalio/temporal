module github.com/temporalio/temporal

go 1.12

require (
	github.com/Shopify/sarama v1.23.0
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7
	github.com/bsm/sarama-cluster v2.1.13+incompatible
	github.com/cactus/go-statsd-client v3.1.1+incompatible
	github.com/cch123/elasticsql v0.0.0-20190321073543-a1a440758eb9
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/emirpasic/gods v0.0.0-20190624094223-e689965507ab
	github.com/fatih/color v0.0.0-20181010231311-3f9d52f7176a
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20171220143535-56a164ee9f31
	github.com/golang/mock v1.3.1
	github.com/google/uuid v1.1.1
	github.com/hashicorp/go-version v1.2.0
	github.com/iancoleman/strcase v0.0.0-20190422225806-e506e3ef7365
	github.com/jmoiron/sqlx v1.2.0
	github.com/jonboulle/clockwork v0.1.0
	github.com/m3db/prometheus_client_golang v0.8.1
	github.com/olekukonko/tablewriter v0.0.1
	github.com/olivere/elastic v6.2.21+incompatible
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pborman/uuid v0.0.0-20180906182336-adf5a7427709
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.2.0
	github.com/stretchr/testify v1.3.0
	github.com/uber-go/kafka-client v0.2.3-0.20191018205945-8b3555b395f9
	github.com/uber-go/tally v3.3.11+incompatible
	github.com/uber/cadence v0.10.0
	github.com/uber/ringpop-go v0.8.5
	github.com/uber/tchannel-go v1.14.0
	github.com/urfave/cli v1.20.0
	github.com/valyala/fastjson v1.4.1
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.uber.org/atomic v1.4.0
	go.uber.org/cadence v0.9.1-0.20191001004132-413d13621ce8
	go.uber.org/config v1.3.1
	go.uber.org/multierr v1.1.0
	go.uber.org/thriftrw v1.20.0
	go.uber.org/yarpc v1.39.0
	go.uber.org/zap v1.10.0
	golang.org/x/net v0.0.0-20190628185345-da137c7871d7
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	gopkg.in/validator.v2 v2.0.0-20180514200540-135c24b11c19
	gopkg.in/yaml.v2 v2.2.2
)

replace github.com/jmoiron/sqlx v1.2.0 => github.com/mfateev/sqlx v0.0.0-20180910213730-fa49b1cf03f7
