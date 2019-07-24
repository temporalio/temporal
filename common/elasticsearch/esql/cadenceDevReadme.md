# ESQL Cadence Usage

## Motivation
Currently [Cadence](https://github.com/uber/cadence) is using [elasticsql](https://github.com/cch123/elasticsql) to translate sql query. However it only support up to ES V2.x while Cadence is using ES V6.x. Beyond that, Cadence has some specific requirements that not supported by elasticsql yet.

Current Cadence query request processing steps are listed below:
- generate SQL from query
- use elasticsql to translate SQL to DSL
- ES V6.x does not support "missing" field, convert "missing" to "bool","must_not","exist" for ExecutionTime query if any
- complete "range" field for ExecutionTime query by adding {"gt": 0}
- add domain query
- key whitelist filtering
- delete some useless field like "from", "size"
- modify sorting field (add workflow id as sorting tie breaker)
- setup search after for pagination

ESQL aims at dealing all these addtional processing steps and providing an api to generate DSL in one step for visibility usage in Cadence.

## Usage
ESQL has convert functions specific for cadence usage. Please refer to `cadencesql.go`. Below shows an example.
Attention: to use cadence version api, `SetCadence()` must be called at initialzation.
~~~~go
sql := "SELECT colA FROM myTable WHERE colB < 10 AND dateTime = '2015-01-01T02:59:59Z'"
domainID := "CadenceSampleDomain"
// custom policy that change colName like "col.." to "myCol.."
func myKeyFilter(colName string) bool {
    return strings.HasPrefix(colName, "col")
}
func myKeyProcess(colName string) (string, error) {
    return "myCol"+colName[3:], nil
}
// custom policy that convert formatted time string to unix nano
func myValueFilter(colName string) bool {
    return strings.Contains(colName, "Time") || strings.Contains(colName, "time")
}
func myValueProcess(timeStr string) (string, error) {
    // convert formatted time string to unix nano integer
    parsedTime, _ := time.Parse(defaultDateTimeFormat, timeStr)
    return fmt.Sprintf("%v", parsedTime.UnixNano()), nil
}
// with the 2 policies , converted dsl is equivalent to
// "SELECT myColA FROM myTable WHERE myColB < 10 AND dateTime = '1561678568048000000'
// in which the time is in unix nano format
e := NewESql()
e.SetCadence()
e.ProcessQueryKey(myKeyFilter, myKeyProcess)         // set up filtering policy
e.ProcessQueryValue(myValueFilter, myValueProcess)     // set up process policy
dsl, _, err := e.ConvertPrettyCadence(sql, domainID)             // convert sql to dsl
if err == nil {
    fmt.Println(dsl)
}
~~~~

## Testing
To setup local testing environment:
- start cassandra service locally. Please refer to [Cadence](https://github.com/uber/cadence) readme.
- start zookeeper and kafka service locally. Here is a [referecne](https://kafka.apache.org/quickstart).
- start elasticsearch and kibana service locally.
- start a cadence worker by `./bin/helloworld -m worker` under cadence directory.
- start cadence service locally. Please refer to [Cadence](https://github.com/uber/cadence) readme.