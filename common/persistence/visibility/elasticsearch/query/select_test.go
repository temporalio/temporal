package query

import (
	"encoding/json"
	"fmt"
	"reflect"
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
	"select * from a,b",
	"select * from a where 1=a",
	"select * from a where id is null",
	"select * from a group by sqrt(id)",
	"select * from aaa where  a= 1 and multi_match(zz=1, query='this is a test', fields=(title,title.origin), type=phrase)",
	"select * from aaa where zz(k=2)",
}

var selectCaseMap = map[string]string{
	// //"select count(*), occupy from callcenter,bbb where id>0 and a=0 and process_id in (1, 2) and a.t = 1 and (b.c=2 or b.d=1) order by aaa desc, ddd asc  limit 1,2",
	"select * from table1 where process_id= 1":                                                       `{"bool":{"filter":{"match_phrase":{"process_id":{"query":"1"}}}}}`,
	"select * from table1 where (process_id= 1)":                                                     `{"bool":{"filter":{"match_phrase":{"process_id":{"query":"1"}}}}}`,
	"select * from table1 where ((process_id= 1))":                                                   `{"bool":{"filter":{"match_phrase":{"process_id":{"query":"1"}}}}}`,
	"select * from table1 where (process_id = 1 and status=1)":                                       `{"bool":{"filter":[{"match_phrase":{"process_id":{"query":"1"}}},{"match_phrase":{"status":{"query":"1"}}}]}}`,
	"select * from table1 where process_id > 1":                                                      `{"bool":{"filter":{"range":{"process_id":{"from":"1","include_lower":false,"include_upper":true,"to":null}}}}}`,
	"select * from table1 where process_id < 1":                                                      `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":false,"to":"1"}}}}}`,
	"select * from table1 where process_id <= 1":                                                     `{"bool":{"filter":{"range":{"process_id":{"from":null,"include_lower":true,"include_upper":true,"to":"1"}}}}}`,
	"select * from table1 where process_id >= '1'":                                                   `{"bool":{"filter":{"range":{"process_id":{"from":"1","include_lower":true,"include_upper":true,"to":null}}}}}`,
	"select * from table1 where process_id != 1":                                                     `{"bool":{"must_not":{"match_phrase":{"process_id":{"query":"1"}}}}}`,
	"select * from table1 where process_id = 0 and status= 1 and channel = 4":                        `{"bool":{"filter":[{"match_phrase":{"process_id":{"query":"0"}}},{"match_phrase":{"status":{"query":"1"}}},{"match_phrase":{"channel":{"query":"4"}}}]}}`,
	"select * from table1 where create_time between '2015-01-01 00:00:00' and '2015-01-01 00:00:00'": `{"bool":{"filter":{"range":{"create_time":{"from":"2015-01-01 00:00:00","include_lower":true,"include_upper":true,"to":"2015-01-01 00:00:00"}}}}}`,
	"select * from table1 where process_id > 1 and status = 1":                                       `{"bool":{"filter":[{"range":{"process_id":{"from":"1","include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"status":{"query":"1"}}}]}}`,
	"select * from table1 where id > 1 or process_id = 0":                                            `{"bool":{"should":[{"range":{"id":{"from":"1","include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"process_id":{"query":"0"}}}]}}`,
	"select * from table1 where id > 1 and d = 1 or process_id = 0 and x = 2":                        `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"from":"1","include_lower":false,"include_upper":true,"to":null}}},{"match_phrase":{"d":{"query":"1"}}}]}},{"bool":{"filter":[{"match_phrase":{"process_id":{"query":"0"}}},{"match_phrase":{"x":{"query":"2"}}}]}}]}}`,

	"select * from table1 where create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'": `{"bool":{"filter":[{"range":{"create_time":{"from":"2015-01-01T00:00:00+0800","include_lower":true,"include_upper":true,"to":"2017-01-01T00:00:00+0800"}}},{"match_phrase":{"process_id":{"query":"0"}}},{"range":{"status":{"from":"1","include_lower":true,"include_upper":true,"to":null}}},{"match_phrase":{"content":{"query":"三个男人"}}},{"match_phrase":{"phone":{"query":"15810324322"}}}]}}`,

	// "select * from table1 where id > 1 order by id asc, order_id desc":                                    `{"bool":{"filter":[{"range":{"id":{"gt":"1"}}}]}},"from":0,"size":1,"sort":[{"id": "asc"},{"order_id": "desc"}]}`,
	// "select * from table1 where (id > 1 and d = 1)":                                                       `{"bool":{"filter":[{"range":{"id":{"gt":"1"}}},{"match_phrase":{"d":{"query":"1"}}}]}}`,
	// "select * from table1 where (id > 1 and d = 1) or (c=1)":                                              `{"bool":{"should":[{"bool":{"filter":[{"range":{"id":{"gt":"1"}}},{"match_phrase":{"d":{"query":"1"}}}]}},{"match_phrase":{"c":{"query":"1"}}}]}}`,
	// "select * from table1 where id > 1 or (process_id = 0)":                                               `{"bool":{"should":[{"range":{"id":{"gt":"1"}}},{"match_phrase":{"process_id":{"query":"0"}}}]}}`,
	// "select * from table1 where id in (1,2,3,4)":                                                          `{"bool":{"filter":[{"terms":{"id":[1, 2, 3, 4]}}]}}`,
	// "select * from table1 where id in ('232', '323') and content = 'aaaa'":                                `{"bool":{"filter":[{"terms":{"id":["232", "323"]}},{"match_phrase":{"content":{"query":"aaaa"}}}]}}`,
	// "select occupy from table1 where create_time between '2015-01-01 00:00:00' and '2014-02-02 00:00:00'": `{"bool":{"filter":[{"range":{"create_time":{"from":"2015-01-01 00:00:00", "to":"2014-02-02 00:00:00"}}}]}}`,
	// "select x from table1 where a like '%a%'":                                                             `{"bool":{"filter":[{"match_phrase":{"a":{"query":"a"}}}]}}`,
	// "select count(*) from table1 group by date_histogram(field='create_time', value='1h')":                `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"date_histogram(field=create_time,value=1h)":{"aggregations":{"COUNT(*)":{"value_count":{"field":"_index"}}},"date_histogram":{"field":"create_time","format":"yyyy-MM-dd HH:mm:ss","interval":"1h"}}}}`,
	// "select * from table1 group by date_histogram(field='create_time', value='1h')":                       `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"date_histogram(field=create_time,value=1h)":{"date_histogram":{"field":"create_time","format":"yyyy-MM-dd HH:mm:ss","interval":"1h"}}}}`,
	// "select * from table1 group by date_histogram(field='create_time', value='4h')":                       `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"date_histogram(field=create_time,value=4h)":{"date_histogram":{"field":"create_time","format":"yyyy-MM-dd HH:mm:ss","interval":"4h"}}}}`,
	// "select * from table1 group by date_histogram(field='create_time', value='1h'), id":                   `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"date_histogram(field=create_time,value=1h)":{"aggregations":{"id":{"terms":{"field":"id","size":0}}},"date_histogram":{"field":"create_time","format":"yyyy-MM-dd HH:mm:ss","interval":"1h"}}}}`,
	// //	"select * from table1 where a like group_concat('%', 'abc', '%')":                                                                                                                                       `{"bool":{"filter":[{"match_phrase":{"a":{"query":"abc"}}}]}}`,
	// "select * from `order`.abcd where `by` = 1":                                `{"bool":{"filter":[{"match_phrase":{"by":{"query":"1"}}}]}}`,
	// "select * from table1 where id not like '%aaa%'":                              `{"bool":{"filter":[{"bool":{"must_not":{"match_phrase":{"id":{"query":"aaa"}}}}}]}}`,
	// "select * from table1 where id not in (1,2,3)":                                `{"bool":{"filter":[{"bool":{"must_not":{"terms":{"id":[1, 2, 3]}}}}]}}`,
	// "select * from abc limit 10,10":                                            `{"bool":{"filter": [{"match_all":{}}]}},"from":10,"size":10}`,
	// "select * from abc limit 10":                                               `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":10}`,
	// "select count(*), id from table1 group by id":                                 `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"id":{"aggregations":{"COUNT(*)":{"value_count":{"field":"_index"}}},"terms":{"field":"id","size":200}}}}`,
	// "SELECT COUNT(distinct age) FROM bank GROUP BY range(age, 20,25,30,35,40)": `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"range(age,20,25,30,35,40)":{"aggregations":{"COUNT(age)":{"cardinality":{"field":"age"}}},"range":{"field":"age","ranges":[{"from":"20","to":"25"},{"from":"25","to":"30"},{"from":"30","to":"35"},{"from":"35","to":"40"}]}}}}`,
	// "select * from a where id != missing":                                      `{"bool":{"filter":[{"bool":{"must_not":[{"missing":{"field":"id"}}]}}]}}`,
	// "select * from a where id = missing":                                       `{"bool":{"filter":[{"missing":{"field":"id"}}]}}`,
	// "select count(*) from a":                                                   `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"COUNT(*)":{"value_count":{"field":"_index"}}}}`,
	// "SELECT online FROM online GROUP BY date_range(field='insert_time' , format='yyyy-MM-dd', '2014-08-18','2014-08-17','now-8d','now-7d','now-6d','now')": `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"date_range(field=insert_time,format=yyyy-MM-dd,2014-08-18,2014-08-17,now-8d,now-7d,now-6d,now)":{"date_range":{"field":"insert_time","format":"yyyy-MM-dd","ranges":[{"from":"2014-08-18","to":"2014-08-17"},{"from":"2014-08-17","to":"now-8d"},{"from":"now-8d","to":"now-7d"},{"from":"now-7d","to":"now-6d"},{"from":"now-6d","to":"now"}]}}}}`,
	// "select count(id), sum(age) from a group by id":                                                                   `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":0,"aggregations":{"id":{"aggregations":{"COUNT(id)":{"value_count":{"field":"id"}},"SUM(age)":{"sum":{"field":"age"}}},"terms":{"field":"id","size":200}}}}`,
	// "select * from a order by `order`.abc":                                                                            `{"bool":{"filter": [{"match_all":{}}]}},"from":0,"size":1,"sort":[{"order.abc": "asc"}]}`,
	// "select * from aaa where multi_match(query='this is a test', fields=(title,title.origin))":                        `{"multi_match":{"query":"this is a test", "fields":["title","title.origin"]}}`,
	// "select * from aaa where  a= 1 and multi_match(query='this is a test', fields=(title,title.origin))":              `{"bool":{"filter":[{"match_phrase":{"a":{"query":"1"}}},{"multi_match":{"query":"this is a test", "fields":["title","title.origin"]}}]}}`,
	// "select * from aaa where  a= 1 and multi_match(query='this is a test', fields=(title,title.origin), type=phrase)": `{"bool":{"filter":[{"match_phrase":{"a":{"query":"1"}}},{"multi_match":{"query":"this is a test", "type":"phrase", "fields":["title","title.origin"]}}]}}`,
}

func TestSupported(t *testing.T) {
	for sql, expectedJson := range selectCaseMap {
		query, _, err := Convert(sql)
		if err != nil {
			t.Error("Convert returned an error", err, sql)
		}

		actualMap, _ := query.Source()

		var expectedMap map[string]interface{}
		_ = json.Unmarshal([]byte(expectedJson), &expectedMap)

		if !reflect.DeepEqual(expectedMap, actualMap) {
			t.Error("The generated query is not equal to the expected")
			actualJson, _ := json.Marshal(actualMap)
			fmt.Println("sql     :", sql)
			fmt.Println("expected:", expectedJson)
			fmt.Println("actual  :", string(actualJson))
		}
	}
}

func TestUnsupported(t *testing.T) {
	for _, v := range unsupportedCaseList {
		_, _, err := Convert(v)

		if err == nil {
			t.Error("can not be true, these cases are not supported!")
		}
	}
}

func TestBadSQL(t *testing.T) {
	for _, v := range badSQLList {
		dsl, table, err := Convert(v)

		if err == nil {
			t.Error("can not be true, these cases are not supported!", v, dsl, table)
		}
	}
}
