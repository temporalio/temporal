// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Copyright (c) 2017 Xargin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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

// Package query is inspired and partially copied from by github.com/cch123/elasticsql.
package query

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var errorCases = map[string]error{
	"delete":                          MalformedSqlQueryErr,
	"update x":                        MalformedSqlQueryErr,
	"insert ":                         MalformedSqlQueryErr,
	"insert into a values(1,2)":       NotSupportedErr,
	"update a set id = 1":             NotSupportedErr,
	"delete from a where id=1":        NotSupportedErr,
	"select * from a where NOT(id=1)": NotSupportedErr,
	"select * from a where 1 = 1":     InvalidExpressionErr,
	"select * from a where 1=a":       InvalidExpressionErr,
	"select * from a where zz(k=2)":   NotSupportedErr,
	"select * from a group by k":      NotSupportedErr,
	"select * from a where  a= 1 and multi_match(zz=1, query='this is a test', fields=(title,title.origin), type=phrase)": NotSupportedErr,
}

var supportedWhereCases = map[string]string{
	"process_id= 1":                 `{"bool":{"filter":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"(process_id= 1)":               `{"bool":{"filter":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"((process_id= 1))":             `{"bool":{"filter":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"(process_id = 1 and status=1)": `{"bool":{"filter":[{"match_phrase":{"process_id":{"query":1}}},{"match_phrase":{"status":{"query":1}}}]}}`,
	"`status`=1":                    `{"bool":{"filter":{"match_phrase":{"status":{"query":1}}}}}`,
	"process_id > 1":                `{"bool":{"filter":{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}}}}`,
	"process_id < 1":                `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":false,"to":1}}}}}`,
	"process_id <= 1":               `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":true,"to":1}}}}}`,
	"process_id >= 1":               `{"bool":{"filter":{"range":{"process_id":{"from":1,"include_lower":true,"include_upper":true,"to":null}}}}}`,
	"process_id != 1":               `{"bool":{"must_not":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"process_id = 0 and status= 1 and channel = 4": `{"bool":{"filter":[{"match_phrase":{"process_id":{"query":0}}},{"match_phrase":{"status":{"query":1}}},{"match_phrase":{"channel":{"query":4}}}]}}`,
	"process_id > 1 and status = 1":                `{"bool":{"filter":[{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"status":{"query":1}}}]}}`,
	"id > 1 or process_id = 0":                     `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"process_id":{"query":0}}}]}}`,
	"id > 1 and d = 1 or process_id = 0 and x = 2": `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"d":{"query":1}}}]}},{"bool":{"filter":[{"match_phrase":{"process_id":{"query":0}}},{"match_phrase":{"x":{"query":2}}}]}}]}}`,
	"(id > 1 and d = 1)":                           `{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"d":{"query":1}}}]}}`,
	"(id > 1 and d = 1) or (c=1)":                  `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"d":{"query":1}}}]}},{"match_phrase":{"c":{"query":1}}}]}}`,
	"id > 1 or (process_id = 0)":                   `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"process_id":{"query":0}}}]}}`,
	"id in (1,2,3,4)":                              `{"bool":{"filter":{"terms":{"id":[1,2,3,4]}}}}`,
	"a = 'text'":                                   `{"bool":{"filter":{"match_phrase":{"a":{"query":"text"}}}}}`,
	"a like '%a%'":                                 `{"bool":{"filter":{"match_phrase":{"a":{"query":"a"}}}}}`,
	"`by` = 1":                                     `{"bool":{"filter":{"match_phrase":{"by":{"query":1}}}}}`,
	"id not like '%aaa%'":                          `{"bool":{"must_not":{"match_phrase":{"id":{"query":"aaa"}}}}}`,
	"id not in (1,2,3)":                            `{"bool":{"must_not":{"terms":{"id":[1,2,3]}}}}`,
	"id is not null":                               `{"bool":{"filter":{"exists":{"field":"id"}}}}`,
	"id is null":                                   `{"bool":{"must_not":{"exists":{"field":"id"}}}}`,
	"id != missing":                                `{"bool":{"filter":{"exists":{"field":"id"}}}}`,
	"id = missing":                                 `{"bool":{"must_not":{"exists":{"field":"id"}}}}`,
	"value = '1'":                                  `{"bool":{"filter":{"match_phrase":{"value":{"query":true}}}}}`,
	"value = 'true'":                               `{"bool":{"filter":{"match_phrase":{"value":{"query":true}}}}}`,
	"value = 'True'":                               `{"bool":{"filter":{"match_phrase":{"value":{"query":true}}}}}`,
	"":                                             `{"bool":{"filter":{"match_all":{}}}}`,
	"id in (\"text1\",'text2') and content = 'aaaa'":                          `{"bool":{"filter":[{"terms":{"id":["text1","text2"]}},{"match_phrase":{"content":{"query":"aaaa"}}}]}}`,
	"create_time between '2015-01-01 00:00:00' and '2016-02-02 00:00:00'":     `{"bool":{"filter":{"range":{"create_time":{"from":"2015-01-01 00:00:00","include_lower":true,"include_upper":true,"to":"2016-02-02 00:00:00"}}}}}`,
	"create_time not between '2015-01-01 00:00:00' and '2016-02-02 00:00:00'": `{"bool":{"must_not":{"range":{"create_time":{"from":"2015-01-01 00:00:00","include_lower":true,"include_upper":true,"to":"2016-02-02 00:00:00"}}}}}`,
	"create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'": `{"bool":{"filter":[{"range":{"create_time":{"from":"2015-01-01T00:00:00+0800","include_lower":true,"include_upper":true,"to":"2017-01-01T00:00:00+0800"}}},{"match_phrase":{"process_id":{"query":0}}},{"range":{"status":{"from":1,"include_lower":true,"include_upper":true,"to":null}}},{"match_phrase":{"content":{"query":"三个男人"}}},{"match_phrase":{"phone":{"query":"15810324322"}}}]}}`,
}

var supportedWhereOrderCases = map[string]struct {
	query  string
	sorter string
}{
	"id > 1 order by id asc, order_id desc": {
		query:  `{"bool":{"filter":{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}}}}`,
		sorter: `[{"id":{"order":"asc"}},{"order_id":{"order":"desc"}}]`,
	},
	"order by `order`.abc": {
		query:  `{"bool":{"filter":{"match_all":{}}}}`,
		sorter: `[{"order.abc":{"order":"asc"}}]`,
	},
}

func TestSupportedSelectWhere(t *testing.T) {
	c := NewConverter(nil, nil)

	for sql, expectedJson := range supportedWhereCases {
		query, _, err := c.ConvertWhereOrderBy(sql)
		assert.NoError(t, err)

		actualMap, _ := query.Source()
		actualJson, _ := json.Marshal(actualMap)

		assert.Equal(t, expectedJson, string(actualJson), fmt.Sprintf("sql: %s", sql))
	}
}

func TestSupportedSelectWhereOrder(t *testing.T) {
	c := NewConverter(nil, nil)

	for sql, expectedJson := range supportedWhereOrderCases {
		query, sorters, err := c.ConvertWhereOrderBy(sql)
		assert.NoError(t, err)

		actualQueryMap, _ := query.Source()
		actualQueryJson, _ := json.Marshal(actualQueryMap)
		assert.Equal(t, expectedJson.query, string(actualQueryJson), fmt.Sprintf("sql: %s", sql))

		var actualSorterMaps []interface{}
		for _, sorter := range sorters {
			actualSorterMap, _ := sorter.Source()
			actualSorterMaps = append(actualSorterMaps, actualSorterMap)
		}
		actualSorterJson, _ := json.Marshal(actualSorterMaps)
		assert.Equal(t, expectedJson.sorter, string(actualSorterJson), fmt.Sprintf("sql: %s", sql))
	}
}

func TestErrors(t *testing.T) {
	c := NewConverter(nil, nil)
	for sql, expectedErr := range errorCases {
		_, _, err := c.convertSql(sql)
		assert.ErrorIs(t, err, expectedErr)
	}
}
