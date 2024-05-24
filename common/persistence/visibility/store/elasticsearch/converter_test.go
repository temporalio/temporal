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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

var errorCases = map[string]string{
	"delete":                                 query.MalformedSqlQueryErrMessage,
	"update x":                               query.MalformedSqlQueryErrMessage,
	"insert ":                                query.MalformedSqlQueryErrMessage,
	"insert into a values(1,2)":              query.NotSupportedErrMessage,
	"update a set id = 1":                    query.NotSupportedErrMessage,
	"delete from a where id=1":               query.NotSupportedErrMessage,
	"select * from a where NOT(id=1)":        query.NotSupportedErrMessage,
	"select * from a where 1 = 1":            query.InvalidExpressionErrMessage,
	"select * from a where 1=a":              query.InvalidExpressionErrMessage,
	"select * from a where zz(k=2)":          query.NotSupportedErrMessage,
	"select * from a group by k, m":          query.NotSupportedErrMessage,
	"select * from a group by k order by id": query.NotSupportedErrMessage,
	"select * from a where a like '%a%'":     "operator 'like' not allowed in comparison expression",
	"select * from a where a not like '%a%'": "operator 'not like' not allowed in comparison expression",
	"invalid query":                          query.MalformedSqlQueryErrMessage,
	"select * from a where  a= 1 and multi_match(zz=1, query='this is a test', fields=(title,title.origin), type=phrase)": query.NotSupportedErrMessage,
}

var supportedWhereCases = map[string]string{
	"process_id= 1":                 `{"bool":{"filter":{"term":{"process_id":1}}}}`,
	"(process_id= 1)":               `{"bool":{"filter":{"term":{"process_id":1}}}}`,
	"((process_id= 1))":             `{"bool":{"filter":{"term":{"process_id":1}}}}`,
	"(process_id = 1 and status=1)": `{"bool":{"filter":[{"term":{"process_id":1}},{"term":{"status":1}}]}}`,
	"`status`=1":                    `{"bool":{"filter":{"term":{"status":1}}}}`,
	"process_id > 1":                `{"bool":{"filter":{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}}}}`,
	"process_id < 1":                `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":false,"to":1}}}}}`,
	"process_id <= 1":               `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":true,"to":1}}}}}`,
	"process_id >= 1":               `{"bool":{"filter":{"range":{"process_id":{"from":1,"include_lower":true,"include_upper":true,"to":null}}}}}`,
	"process_id != 1":               `{"bool":{"must_not":{"term":{"process_id":1}}}}`,
	"process_id = 0 and status= 1 and channel = 4": `{"bool":{"filter":[{"term":{"process_id":0}},{"term":{"status":1}},{"term":{"channel":4}}]}}`,
	"process_id > 1 and status = 1":                `{"bool":{"filter":[{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"term":{"status":1}}]}}`,
	"id > 1 or process_id = 0":                     `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"term":{"process_id":0}}]}}`,
	"id > 1 and d = 1 or process_id = 0 and x = 2": `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"term":{"d":1}}]}},{"bool":{"filter":[{"term":{"process_id":0}},{"term":{"x":2}}]}}]}}`,
	"(id > 1 and d = 1)":                           `{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"term":{"d":1}}]}}`,
	"(id > 1 and d = 1) or (c=1)":                  `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"term":{"d":1}}]}},{"term":{"c":1}}]}}`,
	"nid=1 and (cif = 1 or cif = 2)":               `{"bool":{"filter":[{"term":{"nid":1}},{"bool":{"should":[{"term":{"cif":1}},{"term":{"cif":2}}]}}]}}`,
	"id > 1 or (process_id = 0)":                   `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"term":{"process_id":0}}]}}`,
	"id in (1,2,3,4)":                              `{"bool":{"filter":{"terms":{"id":[1,2,3,4]}}}}`,
	"a = 'text'":                                   `{"bool":{"filter":{"term":{"a":"text"}}}}`,
	"`by` = 1":                                     `{"bool":{"filter":{"term":{"by":1}}}}`,
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
	"create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'": `{"bool":{"filter":[{"range":{"create_time":{"from":"2015-01-01T00:00:00+0800","include_lower":true,"include_upper":true,"to":"2017-01-01T00:00:00+0800"}}},{"term":{"process_id":0}},{"range":{"status":{"from":1,"include_lower":true,"include_upper":true,"to":null}}},{"match":{"content":{"query":"三个男人"}}},{"match":{"phone":{"query":"15810324322"}}}]}}`,
	"value starts_with 'prefix'":     `{"bool":{"filter":{"prefix":{"value":"prefix"}}}}`,
	"value not starts_with 'prefix'": `{"bool":{"must_not":{"prefix":{"value":"prefix"}}}}`,
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

var supportedWhereGroupByCases = map[string]struct {
	query   string
	groupBy []string
}{
	"group by status": {
		query:   ``,
		groupBy: []string{"status"},
	},
	"id = 1 group by status": {
		query:   `{"bool":{"filter":{"term":{"id":1}}}}`,
		groupBy: []string{"status"},
	},
}

var testNameTypeMap = searchattribute.NewNameTypeMapStub(
	map[string]enumspb.IndexedValueType{
		"process_id":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"status":      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"channel":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"id":          enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"nid":         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"a":           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"b":           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"c":           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"d":           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"x":           enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"by":          enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"value":       enumspb.INDEXED_VALUE_TYPE_TEXT,
		"cif":         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"content":     enumspb.INDEXED_VALUE_TYPE_TEXT,
		"create_time": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"phone":       enumspb.INDEXED_VALUE_TYPE_TEXT,
	},
)

func TestSupportedSelectWhere(t *testing.T) {
	c := NewQueryConverter(nil, nil, testNameTypeMap)

	for sql, expectedJson := range supportedWhereCases {
		queryParams, err := c.ConvertWhereOrderBy(sql)
		assert.NoError(t, err)

		actualMap, _ := queryParams.Query.Source()
		actualJson, _ := json.Marshal(actualMap)

		assert.Equal(t, expectedJson, string(actualJson), fmt.Sprintf("sql: %s", sql))
	}
}

func TestEmptySelectWhere(t *testing.T) {
	c := NewQueryConverter(nil, nil, testNameTypeMap)

	queryParams, err := c.ConvertWhereOrderBy("")
	assert.NoError(t, err)
	assert.Nil(t, queryParams.Query)
	assert.Nil(t, queryParams.Sorter)

	queryParams, err = c.ConvertWhereOrderBy("order by Id desc")
	assert.NoError(t, err)
	assert.Nil(t, queryParams.Query)
	assert.Len(t, queryParams.Sorter, 1)
	actualSorterMap, _ := queryParams.Sorter[0].Source()
	actualSorterJson, _ := json.Marshal([]interface{}{actualSorterMap})
	assert.Equal(t, `[{"Id":{"order":"desc"}}]`, string(actualSorterJson))
}

func TestSupportedSelectWhereOrder(t *testing.T) {
	c := NewQueryConverter(nil, nil, testNameTypeMap)

	for sql, expectedJson := range supportedWhereOrderCases {
		queryParams, err := c.ConvertWhereOrderBy(sql)
		assert.NoError(t, err)

		actualQueryMap, _ := queryParams.Query.Source()
		actualQueryJson, _ := json.Marshal(actualQueryMap)
		assert.Equal(t, expectedJson.query, string(actualQueryJson), fmt.Sprintf("sql: %s", sql))

		var actualSorterMaps []interface{}
		for _, sorter := range queryParams.Sorter {
			actualSorterMap, _ := sorter.Source()
			actualSorterMaps = append(actualSorterMaps, actualSorterMap)
		}
		actualSorterJson, _ := json.Marshal(actualSorterMaps)
		assert.Equal(t, expectedJson.sorter, string(actualSorterJson), fmt.Sprintf("sql: %s", sql))
	}
}

func TestSupportedSelectWhereGroupBy(t *testing.T) {
	c := NewQueryConverter(nil, nil, testNameTypeMap)

	for sql, expectedJson := range supportedWhereGroupByCases {
		queryParams, err := c.ConvertWhereOrderBy(sql)
		assert.NoError(t, err)

		if expectedJson.query != "" {
			actualQueryMap, _ := queryParams.Query.Source()
			actualQueryJson, _ := json.Marshal(actualQueryMap)
			assert.Equal(t, expectedJson.query, string(actualQueryJson), fmt.Sprintf("sql: %s", sql))
		} else {
			assert.Nil(t, queryParams.Query)
		}
		assert.Equal(t, expectedJson.groupBy, queryParams.GroupBy)
	}
}

func TestErrors(t *testing.T) {
	c := NewQueryConverter(nil, nil, testNameTypeMap)
	for sql, expectedErrMessage := range errorCases {
		_, err := c.ConvertSql(sql)
		assert.Contains(t, err.Error(), expectedErrMessage, sql)
	}
}
