package query

import (
	"encoding/json"
	"fmt"
	"testing"
)

var badSQLList = []string{
	"delete",
	"update x",
	"insert ",
}

var unsupportedCaseList = []string{
	"insert into a values(1,2)",
	"update a set id = 1",
	"delete from a where id=1",
	"select * from ak where NOT(id=1)",
	"select * from ak where 1 = 1",
	"select * from a where 1=a",
	"select * from aaa where  a= 1 and multi_match(zz=1, query='this is a test', fields=(title,title.origin), type=phrase)",
	"select * from aaa where zz(k=2)",
}

var selectWhereCaseMap = map[string]string{
	"select * from table1 where process_id= 1":                                                       `{"bool":{"filter":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"select * from table1 where (process_id= 1)":                                                     `{"bool":{"filter":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"select * from table1 where ((process_id= 1))":                                                   `{"bool":{"filter":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"select * from table1 where (process_id = 1 and status=1)":                                       `{"bool":{"filter":[{"match_phrase":{"process_id":{"query":1}}},{"match_phrase":{"status":{"query":1}}}]}}`,
	"select * from table1 where process_id > 1":                                                      `{"bool":{"filter":{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}}}}`,
	"select * from table1 where process_id < 1":                                                      `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":false,"to":1}}}}}`,
	"select * from table1 where process_id <= 1":                                                     `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":true,"to":1}}}}}`,
	"select * from table1 where process_id >= '1'":                                                   `{"bool":{"filter":{"range":{"process_id":{"from":"1","include_lower":true,"include_upper":true,"to":null}}}}}`,
	"select * from table1 where process_id != 1":                                                     `{"bool":{"must_not":{"match_phrase":{"process_id":{"query":1}}}}}`,
	"select * from table1 where process_id = 0 and status= 1 and channel = 4":                        `{"bool":{"filter":[{"match_phrase":{"process_id":{"query":0}}},{"match_phrase":{"status":{"query":1}}},{"match_phrase":{"channel":{"query":4}}}]}}`,
	"select * from table1 where create_time between '2015-01-01 00:00:00' and '2015-01-01 00:00:00'": `{"bool":{"filter":{"range":{"create_time":{"from":"2015-01-01 00:00:00","include_lower":true,"include_upper":true,"to":"2015-01-01 00:00:00"}}}}}`,
	"select * from table1 where process_id > 1 and status = 1":                                       `{"bool":{"filter":[{"range":{"process_id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"status":{"query":1}}}]}}`,
	"select * from table1 where id > 1 or process_id = 0":                                            `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"process_id":{"query":0}}}]}}`,
	"select * from table1 where id > 1 and d = 1 or process_id = 0 and x = 2":                        `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"d":{"query":1}}}]}},{"bool":{"filter":[{"match_phrase":{"process_id":{"query":0}}},{"match_phrase":{"x":{"query":2}}}]}}]}}`,
	"select * from table1 where (id > 1 and d = 1)":                                                  `{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"d":{"query":1}}}]}}`,
	"select * from table1 where (id > 1 and d = 1) or (c=1)":                                         `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"d":{"query":1}}}]}},{"match_phrase":{"c":{"query":1}}}]}}`,
	"select * from table1 where id > 1 or (process_id = 0)":                                          `{"bool":{"should":[{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"process_id":{"query":0}}}]}}`,
	"select * from table1 where id in (1,2,3,4)":                                                     `{"bool":{"filter":{"terms":{"id":[1,2,3,4]}}}}`,
	"select * from table1 where id in (\"text1\",'text2') and content = 'aaaa'":                      `{"bool":{"filter":[{"terms":{"id":["text1","text2"]}},{"match_phrase":{"content":{"query":"aaaa"}}}]}}`,
	"select * from table1 where create_time between '2015-01-01 00:00:00' and '2014-02-02 00:00:00'": `{"bool":{"filter":{"range":{"create_time":{"from":"2015-01-01 00:00:00","include_lower":true,"include_upper":true,"to":"2014-02-02 00:00:00"}}}}}`,
	"select * from table1 where a = 'text'":                                                          `{"bool":{"filter":{"match_phrase":{"a":{"query":"text"}}}}}`,
	"select * from table1 where a like '%a%'":                                                        `{"bool":{"filter":{"match_phrase":{"a":{"query":"a"}}}}}`,
	"select * from `order`.abcd where `by` = 1":                                                      `{"bool":{"filter":{"match_phrase":{"by":{"query":1}}}}}`,
	"select * from table1 where id not like '%aaa%'":                                                 `{"bool":{"must_not":{"match_phrase":{"id":{"query":"aaa"}}}}}`,
	"select * from table1 where id not in (1,2,3)":                                                   `{"bool":{"must_not":{"terms":{"id":[1,2,3]}}}}`,
	"select * from table1 where id is not null":                                                      `{"bool":{"filter":{"exists":{"field":"id"}}}}`,
	"select * from table1 where id is null":                                                          `{"bool":{"must_not":{"exists":{"field":"id"}}}}`,
	"select * from table1":                                                                           `{"bool":{"filter":{"match_all":{}}}}`,

	"select * from table1 where create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'": `{"bool":{"filter":[{"range":{"create_time":{"from":"2015-01-01T00:00:00+0800","include_lower":true,"include_upper":true,"to":"2017-01-01T00:00:00+0800"}}},{"match_phrase":{"process_id":{"query":0}}},{"range":{"status":{"from":1,"include_lower":true,"include_upper":true,"to":null}}},{"match_phrase":{"content":{"query":"三个男人"}}},{"match_phrase":{"phone":{"query":"15810324322"}}}]}}`,
}

var selectWhereOrderCaseMap = map[string]struct {
	query  string
	sorter string
}{
	"select * from table1 where id > 1 order by id asc, order_id desc": {
		query:  `{"bool":{"filter":{"range":{"id":{"from":1,"include_lower":false,"include_upper":true,"to":null}}}}}`,
		sorter: `[{"id":{"order":"asc"}},{"order_id":{"order":"desc"}}]`,
	},
	"select * from table1 order by `order`.abc": {
		query:  `{"bool":{"filter":{"match_all":{}}}}`,
		sorter: `[{"order.abc":{"order":"asc"}}]`,
	},
}

func TestSupportedSelectWhere(t *testing.T) {
	for sql, expectedJson := range selectWhereCaseMap {
		query, _, err := Convert(sql)
		if err != nil {
			t.Error("Convert returned an error", err, sql)
		}

		actualMap, _ := query.Source()
		actualJson, _ := json.Marshal(actualMap)

		if expectedJson != string(actualJson) {
			t.Error("The generated query is not equal to the expected")
			fmt.Println("sql     :", sql)
			fmt.Println("expected:", expectedJson)
			fmt.Println("actual  :", string(actualJson))
		}
	}
}

func TestSupportedSelectWhereOrder(t *testing.T) {
	for sql, expectedJson := range selectWhereOrderCaseMap {
		query, sorters, err := Convert(sql)
		if err != nil {
			t.Error("Convert returned an error", err, sql)
		}

		actualQueryMap, _ := query.Source()
		actualQueryJson, _ := json.Marshal(actualQueryMap)
		var actualSorterMaps []interface{}
		for _, sorter := range sorters {
			actualSorterMap, _ := sorter.Source()
			actualSorterMaps = append(actualSorterMaps, actualSorterMap)
		}
		actualSorterJson, _ := json.Marshal(actualSorterMaps)

		if expectedJson.query != string(actualQueryJson) {
			t.Error("The generated query is not equal to the expected")
			fmt.Println("sql     :", sql)
			fmt.Println("expected:", expectedJson.query)
			fmt.Println("actual  :", string(actualQueryJson))
		}

		if expectedJson.sorter != string(actualSorterJson) {
			t.Error("The generated sorter is not equal to the expected")
			fmt.Println("sql     :", sql)
			fmt.Println("expected:", expectedJson.sorter)
			fmt.Println("actual  :", string(actualSorterJson))
		}
	}
}

func TestUnsupported(t *testing.T) {
	for _, v := range unsupportedCaseList {
		_, _, err := Convert(v)

		if err == nil {
			t.Error("can not be true, these cases are not supported!", v)
		}
	}
}

func TestBadSQL(t *testing.T) {
	for _, v := range badSQLList {
		_, _, err := Convert(v)

		if err == nil {
			t.Error("can not be true, these cases are not supported!", v)
		}
	}
}
