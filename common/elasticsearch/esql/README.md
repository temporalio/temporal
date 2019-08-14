# ESQL: Translate SQL to Elasticsearch DSL
Use SQL to query Elasticsearch. ES V6 compatible.

## Supported features
- [x] =, !=, <, >, <=, >=, <>, ()
- [x] comparison with arithmetics (e.g. colA + 1 < colB * 2)
- [x] arithmetic operators: +, -, *, /, %, >>, <<, ()
- [x] AND, OR, NOT
- [x] LIKE, IN, REGEX, IS NULL, BETWEEN
- [x] LIMIT, SIZE, DISTINCT
- [x] COUNT, COUNT(DISTINCT)
- [x] AVG, MAX, MIN, SUM
- [x] GROUP BY, ORDER BY
- [x] HAVING
- [x] query key value macro (see usage)
- [x] pagination (search after)
- [ ] pagination for aggregation
- [ ] functions
- [ ] JOIN
- [ ] nested queries


## Usage
Please refer to code and comments in `esql.go`. `esql.go` contains all the apis that an outside user needs.
### Basic Usage
~~~~go
sql := `SELECT COUNT(*), MAX(colA) FROM myTable WHERE colB < 10 GROUP BY colC HAVING COUNT(*) > 20`
e := NewESql()
dsl, _, err := e.ConvertPretty(sql)    // convert sql to dsl
if err == nil {
    fmt.Println(dsl)
}
~~~~
### Custom Query Macro
ESQL support API `ProcessQueryKey` to register custom policy for colName replacement. It accepts 2 functions, the first function determines whether a colName is to be replaced, the second specifies how to do the replacement. Use case: user has custom field `field`, but to resolve confict, server stores the field as `Custom.field`. `ProcessQueryKey` API can automatically do the conversion.

ESQL support API `ProcessQueryValue` to register custom policy for value processing. It accepts 2 functions, the first function determines whether a value of a colName is to be processed, the second specifies how to do the processing. Use case: user want to query time in readable format, but server stores time as an integer (unix nano). `ProcessQueryValue` API can automatically do the conversion.

Below shows an example.
~~~~go
sql := "SELECT colA FROM myTable WHERE colB < 10 AND dateTime = '2015-01-01T02:59:59Z'"
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
e.ProcessQueryKey(myKeyFilter, myKeyProcess)            // set up macro for key
e.ProcessQueryValue(myValueFilter, myValueProcess)      // set up macro for value
dsl, _, err := e.ConvertPretty(sql)                     // convert sql to dsl
if err == nil {
    fmt.Println(dsl)
}
~~~~
### Pagination
ESQL support 2 kinds of pagination: FROM keyword and ES search_after.
- FROM keyword: the same as SQL syntax. Be careful, **ES only support a page smaller than 10k**, if your offset is large than 10k, search_after is necessary.
- search_after: Once you know the paging tokens, just feed them to `Convert` or `ConvertPretty` API in order.

Below shows an example.
~~~~go
// first page
sql_page1 := "SELECT * FROM myTable ORDER BY colA, colB LIMIT 10"
e := NewESql()
dsl_page1, sortFields, err := e.ConvertPretty(sql_page1)

// second page
// 1. Use FROM to retrieve the 2nd page
sql_page2_FROM := "SELECT * FROM myTable ORDER BY colA, colB LIMIT 10 FROM 10"
dsl_page2_FROM, sortFields, err := e.ConvertPretty(sql_page2_FROM)

// 2. Use search_after to retrieve the 2nd page
// we can use sortFields and the query result from page 1 to get the page tokens
sql_page2_search_after := sql_page1
page_token_colA := "123"
page_token_colB := "bbc"
dsl_page2_search_after, sortFields, err := e.ConvertPretty(sql_page2_search_after, page_colA, page_colB)
~~~~

For Cadence usage, refer to [this link](cadenceDevReadme.md).


## ES V2.x vs ES V6.5
|Item|ES V2.x|ES v6.5|
|:-:|:-:|:-:|
|missing check|{"missing": {"field": "xxx"}}|{"must_not": {"exist": {"field": "xxx"}}}|
|group by multiple columns|nested "aggs" field|"composite" flattened grouping|


## Attentions
- If you want to apply aggregation on some fields, they should not be in type `text` in ES
- `COUNT(colName)` will include documents w/ null values in that column in ES SQL API, while in esql we exclude null valued documents
- ES SQL API and esql do not support `SELECT DISTINCT`, a workaround is to query something like `SELECT * FROM table GROUP BY colName`
- ES SQL API does not support `ORDER BY aggregation`, esql support it by applying bucket_sort
- ES SQL API does not support `HAVING aggregation` that not show up in `SELECT`, esql support it
- To use regex query, the column should be `keyword` type, otherwise the regex is applied to all the terms produced by tokenizer from the original text rather than the original text itself
- Comparison with arithmetics can be potentially slow since it uses scripting query and thus is not able to take advantage of reverse index. For binary operators, please refer to [this link](https://www.elastic.co/guide/en/elasticsearch/painless/6.5/painless-operators.html) on the precedence. We don't support all of them.
- Comparison with arithmetics does not support date type


## Acknowledgement
This project is originated from [elasticsql](https://github.com/cch123/elasticsql). Table below shows the improvement.

|Item|detail|
|:-:|:-:|
|comparison|support comparison with arithmetics of different columns|
|keyword IS|support standard SQL keywords IS NULL, IS NOT NULL for missing check|
|keyword NOT|support NOT, convert NOT recursively since elasticsearch's must_not is not the same as boolean operator NOT in sql|
|keyword LIKE|using "wildcard" tag, support SQL wildcard '%' and '_'|
|keyword REGEX|using "regexp" tag, support standard regular expression syntax|
|keyword GROUP BY|using "composite" tag to flatten multiple grouping|
|keyword ORDER BY|using "bucket_sort" to support order by aggregation functions|
|keyword HAVING|using "bucket_selector" and painless scripting language to support HAVING|
|aggregations|allow introducing aggregation functions from all HAVING, SELECT, ORDER BY|
|column name filtering|allow user pass an white list, when the sql query tries to select column out side white list, refuse the converting|
|column name replacing|allow user pass an function as initializing parameter, the matched column name will be replaced upon the policy|
|query value replacing|allow user pass an function as initializing parameter, query value will be processed by such function if the column name matched in filter function|
|pagination|also return the sorting fields for future search after usage|
|optimization|using "filter" tag rather than "must" tag to avoid scoring analysis and save time|
|optimization|no redundant {"bool": {"filter": xxx}} wrapped|all queries wrapped by {"bool": {"filter": xxx}}|
|optimization|does not return document contents in aggregation query|
|optimization|only return fields user specifies after SELECT|
