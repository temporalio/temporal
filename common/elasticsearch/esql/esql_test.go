// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or `SELl
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package esql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

var sqls = []string{
	`SELECT * FROM test1`,
	`SELECT * FROM test1 ORDER BY colE, colD DESC LIMIT 10 OFFSET 4`,
	`SELECT * FROM test1 WHERE colB = 'ab'`,
	`SELECT * FROM test1 WHERE colB = colBB`,
	`SELECT * FROM test1 WHERE colB = 'ab' AND colB = colBB`,
	`SELECT * FROM test1 WHERE colB = 'ab' AND colB != colBB`,
	`SELECT * FROM test1 WHERE colD = 10`,
	`SELECT * FROM test1 WHERE ( NOT colD = 10)`,
	`SELECT * FROM test1 WHERE ( NOT (colD = 10))`,
	`SELECT * FROM test1 WHERE colD != 10 ORDER BY colD ASC LIMIT 14`,
	`SELECT * FROM test1 WHERE ( NOT colD != 10)`,
	`SELECT * FROM test1 WHERE colD + 1 > 9`,
	`SELECT * FROM test1 WHERE colB = colBB AND 2 * colD > 8+2`,
	`SELECT * FROM test1 WHERE colB + 'c' = colA`,
	`SELECT * FROM test1 WHERE colB + "c" = colA`,
	"SELECT * FROM test1 WHERE `colB` + 'c' = colA",
	`SELECT * FROM test1 WHERE ((colD != 10))`,
	`SELECT * FROM test1 WHERE colB = 'ab' AND ExecutionTime = 2016 AND colB = 'ab'`,
	`SELECT * FROM test1 WHERE colB = 'ab' OR colD = 10`,
	`SELECT * FROM test1 WHERE colD != 10 AND colB = 'bc' OR colB = 'ab'`,
	`SELECT * FROM test1 WHERE colD != 10 AND NOT colB = 'bc' OR NOT (colB = 'ab' AND NOT colE < 10)`,
	`SELECT * FROM test1 WHERE colD != 10 AND (colB = 'bc' OR colB = 'ab')`,
	`SELECT * FROM test1 WHERE (colD != 10 AND colB = 'bc') OR colB = 'ab'`,
	`SELECT * FROM test1 WHERE ((colD != 10) AND (colB = 'bc' OR colB = 'ab'))`,
	`SELECT * FROM test1 WHERE NOT colD != 10 ORDER BY colE DESC, colD DESC`,
	`SELECT * FROM test1 WHERE NOT NOT colD != 10`,
	`SELECT * FROM test1 WHERE NOT NOT NOT colD != 10 ORDER BY colE, colD ASC`,
	`SELECT * FROM test1 WHERE NOT colD != 10 AND colB = 'bc' OR colB = 'ab'`,
	`SELECT * FROM test1 WHERE colD != 10 AND NOT (colB = 'bc' OR colB = 'ab')`,
	`SELECT * FROM test1 WHERE (colD != 10 AND colB = 'bc') OR NOT colB = 'ab'`,
	`SELECT * FROM test1 WHERE NOT ((colD != 10) AND (NOT colB = 'bc' OR colB = 'ab'))`,
	`SELECT * FROM test1 WHERE colE > 3 AND colD <= 15`,
	`SELECT * FROM test1 WHERE colE < 5 OR colD >= 17 ORDER BY colE DESC, colD ASC`,
	`SELECT * FROM test1 WHERE NOT (colE >= 5 AND colD < 17)`,
	`SELECT * FROM test1 WHERE NOT colE >= 5 AND colD < 17`,
	`SELECT * FROM test1 WHERE colE <= 9 OR colD >= 6`,
	`SELECT * FROM test1 WHERE NOT (colE > 9 AND colD < 6)`,
	`SELECT * FROM test1 WHERE colE > 0 OR colD <= 21.000`,
	`SELECT colC FROM test1 WHERE ExecutionTime IS NULL ORDER BY colD`,
	`SELECT * FROM test1 WHERE colB IS NOT NULL ORDER BY colE`,
	`SELECT * FROM test1 WHERE ExecutionTime IS NULL AND colD IS NOT NULL`,
	`SELECT * FROM test1 WHERE NOT ExecutionTime IS NULL OR colD IS NOT NULL`,
	`SELECT * FROM test1 WHERE colB = 'ab' AND (ExecutionTime IS NULL OR colD IS NOT NULL)`,
	`SELECT * FROM test1 WHERE NOT ExecutionTime IS NULL`,
	`SELECT * FROM test1 WHERE NOT ExecutionTime IS NOT NULL`,
	`SELECT ExecutionTime FROM test1 WHERE NOT colE BETWEEN 4 AND 15`,
	`SELECT * FROM test1 WHERE colE BETWEEN 3 AND 12`,
	`SELECT * FROM test1 WHERE NOT colE BETWEEN 3 AND 15 AND colD < 9 OR NOT colB != 'aa'`,
	`SELECT colA FROM test1 WHERE colB IN ('aa', 'ab', 'bb')`,
	`SELECT ExecutionTime FROM test1 WHERE colB NOT IN ('ab', 'bb') AND ExecutionTime IS NOT NULL`,
	`SELECT * FROM test1 WHERE colB NOT IN ('ab', 'bb') AND NOT (colE > 8 OR NOT colD <> 10)`,
	`SELECT colB FROM test1 GROUP BY colB ORDER BY colB`,
	`SELECT colB, colA FROM test1 GROUP BY colB, colB, colA, colA`,
	`SELECT colB FROM test1 WHERE colE > 6 and ExecutionTime IS NOT NULL GROUP BY colB`,
	`SELECT * FROM test1 WHERE colC REGEXP '[ab]{3} a{2}[ab] b+'`,
	`SELECT * FROM test1 WHERE colB LIKE '_a_' OR colB LIKE 'b%'`,
	`SELECT colB, colA FROM test1 GROUP BY colB, colA`,
	`SELECT COUNT(DISTINCT colB) FROM test1`,
	`SELECT COUNT(DISTINCT colB), COUNT(colB) FROM test1`,
	`SELECT COUNT(DISTINCT colB), COUNT(colB), COUNT(DISTINCT colB), COUNT(colB) FROM test1`,
	`SELECT COUNT(colA), COUNT(colB) FROM test1`,
	`SELECT COUNT(*), COUNT(colB) FROM test1`,
	`SELECT COUNT(DISTINCT colB), AVG(colE) FROM test1`,
	`SELECT COUNT(*) FROM test1`,
	`SELECT COUNT(colB) FROM test1`,
	`SELECT MIN(colE) FROM test1`,
	`SELECT colA AS a FROM test1`,
	"SELECT `colA` FROM `test1` WHERE `colB` != 'ab'",
	"SELECT `colA` FROM `test1` WHERE `colB` != \"ab\"",
	"SELECT `colA` FROM `test1` WHERE `colE` BETWEEN 2 AND 10",
	`SELECT COUNT(colB), AVG(colD), MAX(colE) FROM test1`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB`,
	`SELECT AVG(colE), COUNT(*), colB FROM test1 GROUP BY colB, colA`,
	`SELECT AVG(colE), COUNT(ExecutionTime) FROM test1 GROUP BY colB, colA`,
	`SELECT AVG(colE), COUNT(colA) FROM test1 GROUP BY colB, colA`,
	`SELECT COUNT(DISTINCT colE) FROM test1 GROUP BY colB, colA`,
	`SELECT MAX(colD), AVG(colE), COUNT(colA) FROM test1 GROUP BY colB, colA`,
	`SELECT MAX(colD), AVG(colE), COUNT(colA) FROM test1 WHERE ExecutionTime IS NOT NULL GROUP BY colB, colA`,
	`SELECT MAX(colD), AVG(colE), MIN(colD) FROM test1 WHERE ExecutionTime IS NOT NULL AND NOT colD < 2 GROUP BY colB, colA`,
	`SELECT MAX(colD), AVG(colE), MIN(colD) FROM test1 WHERE colE > 1 GROUP BY colB, colA`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY AVG(colE)`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY AVG(colE) DESC`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY AVG(colE) ASC`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY COUNT(*) ASC`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY COUNT(colA) ASC`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY COUNT(colA) ASC, COUNT(colA) ASC`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY COUNT(DISTINCT colA) DESC`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY COUNT(DISTINCT colA) DESC, COUNT(colA), COUNT(*) DESC`,
	`SELECT AVG(colE), COUNT(colE), COUNT(DISTINCT colE) FROM test1 GROUP BY colB ORDER BY COUNT(colE), COUNT(DISTINCT colE) ASC`,
	`SELECT COUNT(DISTINCT colA) FROM test1 GROUP BY colB ORDER BY COUNT(colA), COUNT(*), COUNT(DISTINCT colA)`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB HAVING MAX(colD) > 4`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB HAVING COUNT(colD) > 4`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB HAVING COUNT(*) > 4`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB HAVING COUNT(DISTINCT colA) > 2`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB HAVING MAX(colD) > MIN(colE)`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB HAVING COUNT(DISTINCT colA) > MIN(colE)`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB HAVING MAX(colD) > COUNT(colD)`,
	`SELECT AVG(colE) FROM test1 WHERE colD > 2 GROUP BY colB HAVING MAX(colD) > COUNT(colD) OR MAX(colD) < AVG(colE) AND COUNT(colD) = COUNT(colD) OR COUNT(colD) <> MAX(colD)`,
	`SELECT COUNT(DISTINCT colB) FROM test1 GROUP BY colB`,
	`SELECT AVG(colE) FROM test1 GROUP BY colB ORDER BY MAX(colD)`,
	`SELECT * FROM test1 GROUP BY colB HAVING COUNT(*) > COUNT(colA)`,
	`SELECT * FROM test1 GROUP BY colB HAVING NOT (COUNT(*) > COUNT(colA))`,
	`SELECT * FROM test1 GROUP BY colB HAVING COUNT(*) BETWEEN 0 AND 50`,
}

var dsls = []string{
	`{"size": 1000}`,
	`{"size": 10,"from": 4,"sort": [{"colE": "asc"},{"colD": "desc"}]}`,
	`{"query": {"term": {"colB": "ab"}},"size": 1000}`,
	`{"query": {"bool": {"filter": {"script": {"script": {"source": "doc['colB'].value == doc['colBB'].value"}}}}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"term": {"colB": "ab"}},{"bool": {"filter": {"script": {"script": {"source": "doc['colB'].value == doc['colBB'].value"}}}}}]}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"term": {"colB": "ab"}},{"bool": {"filter": {"script": {"script": {"source": "doc['colB'].value !== doc['colBB'].value"}}}}}]}},"size": 1000}`,
	`{"query": {"term": {"colD": "10"}},"size": 1000}`,
	`{"query": {"bool": {"must_not": {"term": {"colD": "10"}}}},"size": 1000}`,
	`{"query": {"bool": {"must_not": {"term": {"colD": "10"}}}},"size": 1000}`,
	`{"size": 14,"sort": [{"colD": "asc"}],"query": {"bool": {"must_not": {"term": {"colD": "10"}}}}}`,
	`{"query": {"term": {"colD": "10"}},"size": 1000}`,
	`{"query": {"bool": {"filter": {"script": {"script": {"source": "doc['colD'].value + 1 > 9"}}}}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"bool": {"filter": {"script": {"script": {"source": "doc['colB'].value == doc['colBB'].value"}}}}},{"bool": {"filter": {"script": {"script": {"source": "2 * doc['colD'].value > 8 + 2"}}}}}]}},"size": 1000}`,
	`{"query": {"bool": {"filter": {"script": {"script": {"source": "doc['colB'].value + 'c' == doc['colA'].value"}}}}},"size": 1000}`,
	`{"size": 1000,"query": {"bool": {"filter": {"script": {"script": {"source": "doc['colB'].value + 'c' == doc['colA'].value"}}}}}}`,
	`{"query": {"bool": {"filter": {"script": {"script": {"source": "doc['colB'].value + 'c' == doc['colA'].value"}}}}},"size": 1000}`,
	`{"query": {"bool": {"must_not": {"term": {"colD": "10"}}}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"term": {"colB": "ab"}},{"term": {"ExecutionTime": "2016"}},{"term": {"colB": "ab"}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"term": {"colB": "ab"}},{"term": {"colD": "10"}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"bool": {"filter": [{"bool": {"must_not": {"term": {"colD": "10"}}}},{"term": {"colB": "bc"}}]}},{"term": {"colB": "ab"}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"bool": {"filter": [{"bool": {"must_not": {"term": {"colD": "10"}}}},{"bool": {"must_not": {"term": {"colB": "bc"}}}}]}},{"bool": {"must_not": {"term": {"colB": "ab"}}}},{"range": {"colE": {"lt": "10"}}}]}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"bool": {"must_not": {"term": {"colD": "10"}}}},{"bool": {"should": [{"term": {"colB": "bc"}},{"term": {"colB": "ab"}}]}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"bool": {"filter": [{"bool": {"must_not": {"term": {"colD": "10"}}}},{"term": {"colB": "bc"}}]}},{"term": {"colB": "ab"}}]}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"bool": {"must_not": {"term": {"colD": "10"}}}},{"bool": {"should": [{"term": {"colB": "bc"}},{"term": {"colB": "ab"}}]}}]}},"size": 1000}`,
	`{"query": {"term": {"colD": "10"}},"size": 1000,"sort": [{"colE": "desc"},{"colD": "desc"}]}`,
	`{"query": {"bool": {"must_not": {"term": {"colD": "10"}}}},"size": 1000}`,
	`{"query": {"term": {"colD": "10"}},"size": 1000,"sort": [{"colE": "asc"},{"colD": "asc"}]}`,
	`{"query": {"bool": {"should": [{"bool": {"filter": [{"term": {"colD": "10"}},{"term": {"colB": "bc"}}]}},{"term": {"colB": "ab"}}]}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"bool": {"must_not": {"term": {"colD": "10"}}}},{"bool": {"must_not": {"term": {"colB": "bc"}}}},{"bool": {"must_not": {"term": {"colB": "ab"}}}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"bool": {"filter": [{"bool": {"must_not": {"term": {"colD": "10"}}}},{"term": {"colB": "bc"}}]}},{"bool": {"must_not": {"term": {"colB": "ab"}}}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"term": {"colD": "10"}},{"bool": {"filter": [{"term": {"colB": "bc"}},{"bool": {"must_not": {"term": {"colB": "ab"}}}}]}}]}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"range": {"colE": {"gt": "3"}}},{"range": {"colD": {"lte": "15"}}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"range": {"colE": {"lt": "5"}}},{"range": {"colD": {"gte": "17"}}}]}},"size": 1000,"sort": [{"colE": "desc"},{"colD": "asc"}]}`,
	`{"query": {"bool": {"should": [{"range": {"colE": {"lt": "5"}}},{"range": {"colD": {"gte": "17"}}}]}},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"range": {"colE": {"lt": "5"}}},{"range": {"colD": {"lt": "17"}}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"range": {"colE": {"lte": "9"}}},{"range": {"colD": {"gte": "6"}}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"range": {"colE": {"lte": "9"}}},{"range": {"colD": {"gte": "6"}}}]}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"range": {"colE": {"gt": "0"}}},{"range": {"colD": {"lte": "21.000"}}}]}},"size": 1000}`,
	`{"size": 1000,"sort": [{"colD": "asc"}],"query": {"bool": {"must_not": {"exists": {"field": "ExecutionTime"}}}},"_source": {"includes": ["colC"]}}`,
	`{"query": {"exists": {"field": "colB"}},"size": 1000,"sort": [{"colE": "asc"}]}`,
	`{"query": {"bool": {"filter": [{"bool": {"must_not": {"exists": {"field": "ExecutionTime"}}}},{"exists": {"field": "colD"}}]}},"size": 1000}`,
	`{"size": 1000,"query": {"bool": {"should": [{"exists": {"field": "ExecutionTime"}},{"exists": {"field": "colD"}}]}}}`,
	`{"query": {"bool": {"filter": [{"term": {"colB": "ab"}},{"bool": {"should": [{"bool": {"must_not": {"exists": {"field": "ExecutionTime"}}}},{"exists": {"field": "colD"}}]}}]}},"size": 1000}`,
	`{"query": {"exists": {"field": "ExecutionTime"}},"size": 1000}`,
	`{"size": 1000,"query": {"bool": {"must_not": {"exists": {"field": "ExecutionTime"}}}}}`,
	`{"query": {"bool": {"must_not": [{"range": {"colE": {"gte": "4", "lte": "15"}}}]}},"_source": {"includes": ["ExecutionTime"]},"size": 1000}`,
	`{"query": {"range": {"colE": {"gte": "3", "lte": "12"}}},"size": 1000}`,
	`{"query": {"bool": {"should": [{"bool": {"filter": [{"bool": {"must_not": [{"range": {"colE": {"gte": "3", "lte": "15"}}}]}},{"range": {"colD": {"lt": "9"}}}]}},{"term": {"colB": "aa"}}]}},"size": 1000}`,
	`{"query": {"terms": {"colB": ["aa", "ab", "bb"]}},"_source": {"includes": ["colA"]},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"bool": {"must_not": {"terms": {"colB": ["ab", "bb"]}}}},{"exists": {"field": "ExecutionTime"}}]}},"_source": {"includes": ["ExecutionTime"]},"size": 1000}`,
	`{"query": {"bool": {"filter": [{"bool": {"must_not": {"terms": {"colB": ["ab", "bb"]}}}},{"range": {"colE": {"lte": "8"}}},{"bool": {"must_not": {"term": {"colD": "10"}}}}]}},"size": 1000}`,
	`{"_source": {"includes": ["colB"]},"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}}},"size": 0,"_source": {"includes": ["colB", "colA"]}}`,
	`{"query": {"bool": {"filter": [{"range": {"colE": {"gt": "6"}}},{"exists": {"field": "ExecutionTime"}}]}},"_source": {"includes": ["colB"]},"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}}},"size": 0}`,
	`{"query": {"regexp": {"colC": "[ab]{3} a{2}[ab] b+"}},"size": 1000}`,
	`{"size": 1000,"query": {"bool": {"should": [{"wildcard": {"colB": {"wildcard": "?a?"}}},{"wildcard": {"colB": {"wildcard": "b*"}}}]}}}`,
	`{"_source": {"includes": ["colB", "colA"]},"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}}},"size": 0}`,
	`{"size": 0,"aggs": {"count_distinct_colB": {"cardinality": {"field": "colB"}}}}`,
	`{"size": 0,"aggs": {"count_distinct_colB": {"cardinality": {"field": "colB"}},"count_colB": {"value_count": {"field": "colB"}}}}`,
	`{"aggs": {"count_distinct_colB": {"cardinality": {"field": "colB"}},"count_colB": {"value_count": {"field": "colB"}}},"size": 0}`,
	`{"size": 0,"aggs": {"count_colA": {"value_count": {"field": "colA"}},"count_colB": {"value_count": {"field": "colB"}}}}`,
	`{"aggs": {"count_colB": {"value_count": {"field": "colB"}}},"size": 0}`,
	`{"size": 0,"aggs": {"count_distinct_colB": {"cardinality": {"field": "colB"}},"avg_colE": {"avg": {"field": "colE"}}}}`,
	`{"size": 0}`,
	`{"aggs": {"count_colB": {"value_count": {"field": "colB"}}},"size": 0}`,
	`{"aggs": {"min_colE": {"min": {"field": "colE"}}},"size": 0}`,
	`{"_source": {"includes": ["colA"]},"size": 1000}`,
	`{"_source": {"includes": ["colA"]},"size": 1000,"query": {"bool": {"must_not": {"term": {"colB": "ab"}}}}}`,
	`{"query": {"bool": {"must_not": {"term": {"colB": "ab"}}}},"_source": {"includes": ["colA"]},"size": 1000}`,
	`{"_source": {"includes": ["colA"]},"size": 1000,"query": {"range": {"colE": {"gte": "2", "lte": "10"}}}}`,
	`{"aggs": {"count_colB": {"value_count": {"field": "colB"}},"avg_colD": {"avg": {"field": "colD"}},"max_colE": {"max": {"field": "colE"}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}}}}},"size": 0}`,
	`{"_source": {"includes": ["colB"]},"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_ExecutionTime": {"value_count": {"field": "ExecutionTime"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_colA": {"value_count": {"field": "colA"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"count_distinct_colE": {"cardinality": {"field": "colE"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"max_colD": {"max": {"field": "colD"}},"avg_colE": {"avg": {"field": "colE"}},"count_colA": {"value_count": {"field": "colA"}}}}},"size": 0}`,
	`{"query": {"exists": {"field": "ExecutionTime"}},"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"max_colD": {"max": {"field": "colD"}},"avg_colE": {"avg": {"field": "colE"}},"count_colA": {"value_count": {"field": "colA"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"max_colD": {"max": {"field": "colD"}},"avg_colE": {"avg": {"field": "colE"}},"min_colD": {"min": {"field": "colD"}}}}},"size": 0,"query": {"bool": {"filter": [{"exists": {"field": "ExecutionTime"}},{"range": {"colD": {"gte": "2"}}}]}}}`,
	`{"query": {"range": {"colE": {"gt": "1"}}},"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}},{"group_colA": {"terms": {"field": "colA", "missing_bucket": true}}}]}, "aggs": {"max_colD": {"max": {"field": "colD"}},"avg_colE": {"avg": {"field": "colE"}},"min_colD": {"min": {"field": "colD"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"bucket_sort": {"bucket_sort": {"sort": [{"avg_colE": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"bucket_sort": {"bucket_sort": {"sort": [{"avg_colE": {"order": "desc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"bucket_sort": {"bucket_sort": {"sort": [{"avg_colE": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"bucket_sort": {"bucket_sort": {"sort": [{"_count": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_colA": {"value_count": {"field": "colA"}},"bucket_sort": {"bucket_sort": {"sort": [{"count_colA": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_colA": {"value_count": {"field": "colA"}},"bucket_sort": {"bucket_sort": {"sort": [{"count_colA": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"size": 0,"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_distinct_colA": {"cardinality": {"field": "colA"}},"bucket_sort": {"bucket_sort": {"sort": [{"count_distinct_colA": {"order": "desc"}}], "size": 1000}}}}}}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_distinct_colA": {"cardinality": {"field": "colA"}},"count_colA": {"value_count": {"field": "colA"}},"bucket_sort": {"bucket_sort": {"sort": [{"count_distinct_colA": {"order": "desc"}},{"count_colA": {"order": "asc"}},{"_count": {"order": "desc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_colE": {"value_count": {"field": "colE"}},"count_distinct_colE": {"cardinality": {"field": "colE"}},"bucket_sort": {"bucket_sort": {"sort": [{"count_colE": {"order": "asc"}},{"count_distinct_colE": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"count_distinct_colA": {"cardinality": {"field": "colA"}},"count_colA": {"value_count": {"field": "colA"}},"bucket_sort": {"bucket_sort": {"sort": [{"count_colA": {"order": "asc"}},{"_count": {"order": "asc"}},{"count_distinct_colA": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"max_colD": {"max": {"field": "colD"}},"having": {"bucket_selector": {"buckets_path": {"max_colD": "max_colD"}, "script": "params.max_colD > 4"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_colD": {"value_count": {"field": "colD"}},"having": {"bucket_selector": {"buckets_path": {"count_colD": "count_colD"}, "script": "params.count_colD > 4"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"having": {"bucket_selector": {"buckets_path": {"_count": "_count"}, "script": "params._count > 4"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_distinct_colA": {"cardinality": {"field": "colA"}},"having": {"bucket_selector": {"buckets_path": {"count_distinct_colA": "count_distinct_colA"}, "script": "params.count_distinct_colA > 2"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"max_colD": {"max": {"field": "colD"}},"min_colE": {"min": {"field": "colE"}},"having": {"bucket_selector": {"buckets_path": {"max_colD": "max_colD","min_colE": "min_colE"}, "script": "params.max_colD > params.min_colE"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"count_distinct_colA": {"cardinality": {"field": "colA"}},"min_colE": {"min": {"field": "colE"}},"having": {"bucket_selector": {"buckets_path": {"count_distinct_colA": "count_distinct_colA","min_colE": "min_colE"}, "script": "params.count_distinct_colA > params.min_colE"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"max_colD": {"max": {"field": "colD"}},"count_colD": {"value_count": {"field": "colD"}},"having": {"bucket_selector": {"buckets_path": {"count_colD": "count_colD","max_colD": "max_colD"}, "script": "params.max_colD > params.count_colD"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"max_colD": {"max": {"field": "colD"}},"count_colD": {"value_count": {"field": "colD"}},"having": {"bucket_selector": {"buckets_path": {"avg_colE": "avg_colE","max_colD": "max_colD","count_colD": "count_colD"}, "script": "params.max_colD > params.count_colD || params.max_colD < params.avg_colE && params.count_colD == params.count_colD || params.count_colD !== params.max_colD"}}}}},"size": 0,"query": {"range": {"colD": {"gt": "2"}}}}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"count_distinct_colB": {"cardinality": {"field": "colB"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"avg_colE": {"avg": {"field": "colE"}},"max_colD": {"max": {"field": "colD"}},"bucket_sort": {"bucket_sort": {"sort": [{"max_colD": {"order": "asc"}}], "size": 1000}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"count_colA": {"value_count": {"field": "colA"}},"having": {"bucket_selector": {"buckets_path": {"_count": "_count","count_colA": "count_colA"}, "script": "params._count > params.count_colA"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"count_colA": {"value_count": {"field": "colA"}},"having": {"bucket_selector": {"buckets_path": {"_count": "_count","count_colA": "count_colA"}, "script": "!(params._count > params.count_colA)"}}}}},"size": 0}`,
	`{"aggs": {"groupby": {"composite": {"size": 1000, "sources": [{"group_colB": {"terms": {"field": "colB", "missing_bucket": true}}}]}, "aggs": {"having": {"bucket_selector": {"buckets_path": {"_count": "_count"}, "script": "(params._count >= 0 && params._count <= 50)"}}}}},"size": 0}`,
}

func TestUnit(t *testing.T) {
	e := NewESql()
	for i, sql := range sqls {
		fmt.Printf("test %dth query ...\n", i+1)
		dsl, _, err := e.Convert(sql)
		if err != nil {
			t.Errorf("%vth query fails: %v", i+1, err)
			return
		}
		var dslMap, dslMapRef map[string]interface{}
		err = json.Unmarshal([]byte(dsl), &dslMap)
		if err != nil {
			t.Errorf("%vth query fails", i+1)
			return
		}
		err = json.Unmarshal([]byte(dsls[i]), &dslMapRef)
		if err != nil {
			t.Errorf("%vth query reference fails", i+1)
			return
		}
		if !reflect.DeepEqual(dslMap, dslMapRef) {
			t.Errorf("%vth query does not match", i+1)
			return
		}
	}
}
