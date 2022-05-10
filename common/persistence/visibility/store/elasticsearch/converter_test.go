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

package elasticsearch

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/persistence/visibility/store/query"
)

var errorCases = map[string]string{
	"delete":                          query.MalformedSqlQueryErrMessage,
	"update x":                        query.MalformedSqlQueryErrMessage,
	"insert ":                         query.MalformedSqlQueryErrMessage,
	"insert into a values(1,2)":       query.NotSupportedErrMessage,
	"update a set id = 1":             query.NotSupportedErrMessage,
	"delete from a where id=1":        query.NotSupportedErrMessage,
	"select * from a where NOT(id=1)": query.NotSupportedErrMessage,
	"select * from a where 1 = 1":     query.InvalidExpressionErrMessage,
	"select * from a where 1=a":       query.InvalidExpressionErrMessage,
	"select * from a where zz(k=2)":   query.NotSupportedErrMessage,
	"select * from a group by k":      query.NotSupportedErrMessage,
	"invalid query":                   query.MalformedSqlQueryErrMessage,
	"select * from a where  a= 1 and multi_match(zz=1, query='this is a test', fields=(title,title.origin), type=phrase)": query.NotSupportedErrMessage,
}

var supportedWhereCases = map[string]string{
	"process_id= 1":                 `{"bool":{"filter":{"match":{"process_id":{"query":1}}}}}`,
	"(process_id= 1)":               `{"bool":{"filter":{"match":{"process_id":{"query":1}}}}}`,
	"((process_id= 1))":             `{"bool":{"filter":{"match":{"process_id":{"query":1}}}}}`,
	"(process_id = 1 and status=1)": `{"bool":{"filter":[{"match":{"process_id":{"query":1}}},{"match":{"status":{"query":1}}}]}}`,
	"`status`=1":                    `{"bool":{"filter":{"match":{"status":{"query":1}}}}}`,
	"process_id > 1":                `{"bool":{"filter":{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}}}}`,
	"process_id < 1":                `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":false,"to":1}}}}}`,
	"process_id <= 1":               `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":true,"to":1}}}}}`,
	"process_id >= 1":               `{"bool":{"filter":{"range":{"process_id":{"from":1,"include_lower":true,"include_upper":true,"to":null}}}}}`,
	"process_id != 1":               `{"bool":{"must_not":{"match":{"process_id":{"query":1}}}}}`,
	"process_id = 0 and status= 1 and channel = 4": `{"bool":{"filter":[{"match":{"process_id":{"query":0}}},{"match":{"status":{"query":1}}},{"match":{"channel":{"query":4}}}]}}`,
	"process_id > 1 and status = 1":                `{"bool":{"filter":[{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match":{"status":{"query":1}}}]}}`,
	"id > 1 or process_id = 0":                     `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match":{"process_id":{"query":0}}}]}}`,
	"id > 1 and d = 1 or process_id = 0 and x = 2": `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match":{"d":{"query":1}}}]}},{"bool":{"filter":[{"match":{"process_id":{"query":0}}},{"match":{"x":{"query":2}}}]}}]}}`,
	"(id > 1 and d = 1)":                           `{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match":{"d":{"query":1}}}]}}`,
	"(id > 1 and d = 1) or (c=1)":                  `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match":{"d":{"query":1}}}]}},{"match":{"c":{"query":1}}}]}}`,
	"nid=1 and (cif = 1 or cif = 2)":               `{"bool":{"filter":[{"match":{"nid":{"query":1}}},{"bool":{"should":[{"match":{"cif":{"query":1}}},{"match":{"cif":{"query":2}}}]}}]}}`,
	"id > 1 or (process_id = 0)":                   `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match":{"process_id":{"query":0}}}]}}`,
	"id in (1,2,3,4)":                              `{"bool":{"filter":{"terms":{"id":[1,2,3,4]}}}}`,
	"a = 'text'":                                   `{"bool":{"filter":{"match":{"a":{"query":"text"}}}}}`,
	"a LiKE '%a%'":                                 `{"bool":{"filter":{"match":{"a":{"query":"a"}}}}}`,
	"`by` = 1":                                     `{"bool":{"filter":{"match":{"by":{"query":1}}}}}`,
	"id not like '%aaa%'":                          `{"bool":{"must_not":{"match":{"id":{"query":"aaa"}}}}}`,
	"id not IN (1, 2,3)":                           `{"bool":{"must_not":{"terms":{"id":[1,2,3]}}}}`,
	"id iS not null":                               `{"bool":{"filter":{"exists":{"field":"id"}}}}`,
	"id is NULL":                                   `{"bool":{"must_not":{"exists":{"field":"id"}}}}`,
	"value = '1'":                                  `{"bool":{"filter":{"match":{"value":{"query":"1"}}}}}`,
	"value = 'true'":                               `{"bool":{"filter":{"match":{"value":{"query":"true"}}}}}`,
	"value = 'True'":                               `{"bool":{"filter":{"match":{"value":{"query":"True"}}}}}`,
	"value = true":                                 `{"bool":{"filter":{"match":{"value":{"query":true}}}}}`,
	"value = True":                                 `{"bool":{"filter":{"match":{"value":{"query":true}}}}}`,
	"value = 1528358645123456789":                  `{"bool":{"filter":{"match":{"value":{"query":1528358645123456789}}}}}`,
	"value = 1528358645.1234567":                   `{"bool":{"filter":{"match":{"value":{"query":1528358645.1234567}}}}}`,
	// Long float is truncated.
	"value = 1528358645.123456790":                                            `{"bool":{"filter":{"match":{"value":{"query":1528358645.1234567}}}}}`,
	"id in (\"text1\",'text2') and content = 'aaaa'":                          `{"bool":{"filter":[{"terms":{"id":["text1","text2"]}},{"match":{"content":{"query":"aaaa"}}}]}}`,
	"create_time BETWEEN '2015-01-01 00:00:00' and '2016-02-02 00:00:00'":     `{"bool":{"filter":{"range":{"create_time":{"from":"2015-01-01 00:00:00","include_lower":true,"include_upper":true,"to":"2016-02-02 00:00:00"}}}}}`,
	"create_time nOt between '2015-01-01 00:00:00' and '2016-02-02 00:00:00'": `{"bool":{"must_not":{"range":{"create_time":{"from":"2015-01-01 00:00:00","include_lower":true,"include_upper":true,"to":"2016-02-02 00:00:00"}}}}}`,
	"create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'": `{"bool":{"filter":[{"range":{"create_time":{"from":"2015-01-01T00:00:00+0800","include_lower":true,"include_upper":true,"to":"2017-01-01T00:00:00+0800"}}},{"match":{"process_id":{"query":0}}},{"range":{"status":{"from":1,"include_lower":true,"include_upper":true,"to":null}}},{"match":{"content":{"query":"三个男人"}}},{"match":{"phone":{"query":"15810324322"}}}]}}`,
}

var supportedWhereOrderCases = map[string]struct {
	query  string
	sorter string
}{
	"id > 1 order by id asc, order_id desc": {
		query:  `{"bool":{"filter":{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}}}}`,
		sorter: `[{"id":{"order":"asc"}},{"order_id":{"order":"desc"}}]`,
	},
	"id is null order by `order`.abc": {
		query:  `{"bool":{"must_not":{"exists":{"field":"id"}}}}`,
		sorter: `[{"order.abc":{"order":"asc"}}]`,
	},
	"id beTweeN 1 AnD 3 ORdeR BY random_id DESC": {
		query:  `{"bool":{"filter":{"range":{"id":{"from":1,"include_lower":true,"include_upper":true,"to":3}}}}}`,
		sorter: `[{"random_id":{"order":"desc"}}]`,
	},
}

func TestSupportedSelectWhere(t *testing.T) {
	c := newQueryConverter(nil, nil)

	for sql, expectedJson := range supportedWhereCases {
		query, _, err := c.ConvertWhereOrderBy(sql)
		assert.NoError(t, err)

		actualMap, _ := query.Source()
		actualJson, _ := json.Marshal(actualMap)

		assert.Equal(t, expectedJson, string(actualJson), fmt.Sprintf("sql: %s", sql))
	}
}

func TestEmptySelectWhere(t *testing.T) {
	c := newQueryConverter(nil, nil)

	query, sorters, err := c.ConvertWhereOrderBy("")
	assert.NoError(t, err)
	assert.Nil(t, query)
	assert.Nil(t, sorters)

	query, sorters, err = c.ConvertWhereOrderBy("order by Id desc")
	assert.NoError(t, err)
	assert.Nil(t, query)
	assert.Len(t, sorters, 1)
	actualSorterMap, _ := sorters[0].Source()
	actualSorterJson, _ := json.Marshal([]interface{}{actualSorterMap})
	assert.Equal(t, `[{"Id":{"order":"desc"}}]`, string(actualSorterJson))
}

func TestSupportedSelectWhereOrder(t *testing.T) {
	c := newQueryConverter(nil, nil)

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
	c := newQueryConverter(nil, nil)
	for sql, expectedErrMessage := range errorCases {
		_, _, err := c.ConvertSql(sql)
		assert.Contains(t, err.Error(), expectedErrMessage, sql)
	}
}
