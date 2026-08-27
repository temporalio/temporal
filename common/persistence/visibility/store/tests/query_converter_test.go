package tests

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	vissql "go.temporal.io/server/common/persistence/visibility/store/sql"
	"go.temporal.io/server/common/searchattribute"
)

const (
	testNamespaceName = namespace.Name("test-namespace")

	mysqlStore    = "mysql8"
	postgresStore = "postgres12"
	sqliteStore   = "sqlite"
	esStore       = "elasticsearch"
)

var sqlPlugins = []string{mysqlStore, postgresStore, sqliteStore}

type queryConverterTestCase struct {
	name string
	in   string

	// out is the expected converted query: `sql` is the WHERE clause expected for every SQL
	// plugin, and `mysql`, `postgres` and `sqlite` override it for the plugins whose output
	// differs (mostly Text and KeywordList types); `es` is the expected Elasticsearch query.
	sql      string
	mysql    string
	postgres string
	sqlite   string
	es       string

	// err is the expected error message for every store: most errors come from the shared
	// front-end converter. When only some stores fail, use the store specific fields
	// instead: sqlErr for every SQL plugin, and mysqlErr, postgresErr, sqliteErr and esErr
	// for a single store.
	err         string
	sqlErr      string
	mysqlErr    string
	postgresErr string
	sqliteErr   string
	esErr       string

	// groupBy is the expected list of GROUP BY search attribute field names.
	groupBy []string
	// orderBy is the expected ORDER BY clause.
	orderBy string
}

func (tc *queryConverterTestCase) expected(store string) (out string, errMsg string) {
	if tc.err != "" {
		return "", tc.err
	}
	if store == esStore {
		return tc.es, tc.esErr
	}

	var outOverride, errOverride string
	switch store {
	case mysqlStore:
		outOverride, errOverride = tc.mysql, tc.mysqlErr
	case postgresStore:
		outOverride, errOverride = tc.postgres, tc.postgresErr
	case sqliteStore:
		outOverride, errOverride = tc.sqlite, tc.sqliteErr
	default:
		// no-op
	}

	if errOverride != "" {
		return "", errOverride
	}
	if outOverride != "" {
		return outOverride, ""
	}
	return tc.sql, tc.sqlErr
}

var queryConverterTestCases = []queryConverterTestCase{
	{
		name: "empty query",
		in:   "",
		sql:  "TemporalNamespaceDivision is null",
		es:   `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "blank query",
		in:   "   ",
		sql:  "TemporalNamespaceDivision is null",
		es:   `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	// Keyword type search attributes.
	{
		name: "Keyword equal",
		in:   "WorkflowId = 'wid'",
		sql:  "TemporalNamespaceDivision is null and workflow_id = 'wid'",
		es:   `{"bool":{"filter":{"term":{"WorkflowId":"wid"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword not equal",
		in:   "WorkflowId != 'wid'",
		sql:  "TemporalNamespaceDivision is null and workflow_id != 'wid'",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"term":{"WorkflowId":"wid"}}]}}`,
	},
	{
		name: "Keyword in",
		in:   "WorkflowType IN ('foo', 'bar')",
		sql:  "TemporalNamespaceDivision is null and workflow_type_name in ('foo', 'bar')",
		es:   `{"bool":{"filter":{"terms":{"WorkflowType":["foo","bar"]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword not in",
		in:   "WorkflowType NOT IN ('foo', 'bar')",
		sql:  "TemporalNamespaceDivision is null and workflow_type_name not in ('foo', 'bar')",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"terms":{"WorkflowType":["foo","bar"]}}]}}`,
	},
	{
		name: "Keyword starts with",
		in:   "TaskQueue STARTS_WITH 'foo'",
		sql:  "TemporalNamespaceDivision is null and task_queue like 'foo%' escape '!'",
		es:   `{"bool":{"filter":{"prefix":{"TaskQueue":"foo"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword not starts with",
		in:   "TaskQueue NOT STARTS_WITH 'foo'",
		sql:  "TemporalNamespaceDivision is null and task_queue not like 'foo%' escape '!'",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"prefix":{"TaskQueue":"foo"}}]}}`,
	},
	{
		name: "Keyword starts with special chars",
		in:   "AliasForKeyword01 STARTS_WITH 'a%b_c!d'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 like 'a!%b!_c!!d%' escape '!'",
		es:   `{"bool":{"filter":{"prefix":{"Keyword01":"a%b_c!d"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword starts with non string value",
		in:   "AliasForKeyword01 STARTS_WITH 123",
		err:  "invalid expression: invalid value type for search attribute AliasForKeyword01 of type Keyword: 123 (type: int64)",
	},
	{
		name: "Keyword greater than",
		in:   "AliasForKeyword01 > 'foo'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 > 'foo'",
		es:   `{"bool":{"filter":{"range":{"Keyword01":{"from":"foo","include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword between",
		in:   "AliasForKeyword01 BETWEEN 'a' AND 'b'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 between 'a' and 'b'",
		es:   `{"bool":{"filter":{"range":{"Keyword01":{"from":"a","include_lower":true,"include_upper":true,"to":"b"}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword is null",
		in:   "AliasForKeyword01 IS NULL",
		sql:  "TemporalNamespaceDivision is null and Keyword01 is null",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"exists":{"field":"Keyword01"}}]}}`,
	},
	{
		name: "Keyword is not null",
		in:   "AliasForKeyword01 IS NOT NULL",
		sql:  "TemporalNamespaceDivision is null and Keyword01 is not null",
		es:   `{"bool":{"filter":{"exists":{"field":"Keyword01"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword empty string",
		in:   "AliasForKeyword01 = ''",
		sql:  "TemporalNamespaceDivision is null and Keyword01 = ''",
		es:   `{"bool":{"filter":{"term":{"Keyword01":""}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword value with quote",
		in:   "AliasForKeyword01 = 'foo''s bar'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 = 'foo''s bar'",
		es:   `{"bool":{"filter":{"term":{"Keyword01":"foo's bar"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword value with backslash",
		in:   "AliasForKeyword01 = 'foo\\\\bar'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 = 'foo\\\\bar'",
		es:   `{"bool":{"filter":{"term":{"Keyword01":"foo\\bar"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword invalid value type",
		in:   "AliasForKeyword01 = 123",
		err:  "invalid expression: invalid value type for search attribute AliasForKeyword01 of type Keyword: 123 (type: int64)",
	},
	{
		name: "Keyword with bool value",
		in:   "AliasForKeyword01 = true",
		err:  "invalid expression: invalid value type for search attribute AliasForKeyword01 of type Keyword: true (type: bool)",
	},
	{
		name: "Keyword with negative value",
		in:   "AliasForKeyword01 = -'foo'",
		err:  `invalid expression: unary operator not supported in "-'foo'"`,
	},
	{
		name: "Keyword unsupported operator",
		in:   "AliasForKeyword01 LIKE 'foo%'",
		err:  "operation is not supported: operator 'LIKE' not supported for Keyword type search attribute 'AliasForKeyword01'",
	},
	{
		name: "Keyword value with escaped chars",
		in:   "AliasForKeyword01 = 'foo\\nbar\\ttail'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 = 'foo\\nbar\\ttail'",
		es:   `{"bool":{"filter":{"term":{"Keyword01":"foo\nbar\ttail"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword single value in list",
		in:   "WorkflowId IN ('wid')",
		sql:  "TemporalNamespaceDivision is null and workflow_id in ('wid')",
		es:   `{"bool":{"filter":{"terms":{"WorkflowId":["wid"]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Keyword empty list",
		in:   "WorkflowId IN ()",
		err:  "malformed SQL query: syntax error at position 44",
	},
	{
		name: "search attribute name is case sensitive",
		in:   "workflowid = 'wid'",
		err:  "invalid expression: column name 'workflowid' is not a valid search attribute",
	},

	// Int type search attributes.
	{
		name: "Int equal",
		in:   "HistoryLength = 10",
		sql:  "TemporalNamespaceDivision is null and history_length = 10",
		es:   `{"bool":{"filter":{"term":{"HistoryLength":10}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int negative value",
		in:   "AliasForInt01 = -10",
		sql:  "TemporalNamespaceDivision is null and Int01 = -10",
		es:   `{"bool":{"filter":{"term":{"Int01":-10}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		// Only a literal value can be negated, not an arbitrary expression.
		name: "Int negative parenthesized value",
		in:   "AliasForInt01 = -(10)",
		err:  "invalid expression: unexpected value type *sqlparser.ParenExpr",
	},
	{
		name: "Int greater than",
		in:   "HistoryLength > 10",
		sql:  "TemporalNamespaceDivision is null and history_length > 10",
		es:   `{"bool":{"filter":{"range":{"HistoryLength":{"from":10,"include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int greater equal",
		in:   "HistoryLength >= 10",
		sql:  "TemporalNamespaceDivision is null and history_length >= 10",
		es:   `{"bool":{"filter":{"range":{"HistoryLength":{"from":10,"include_lower":true,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int less than",
		in:   "StateTransitionCount < 10",
		sql:  "TemporalNamespaceDivision is null and state_transition_count < 10",
		es:   `{"bool":{"filter":{"range":{"StateTransitionCount":{"from":null,"include_lower":true,"include_upper":false,"to":10}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int less equal",
		in:   "StateTransitionCount <= 10",
		sql:  "TemporalNamespaceDivision is null and state_transition_count <= 10",
		es:   `{"bool":{"filter":{"range":{"StateTransitionCount":{"from":null,"include_lower":true,"include_upper":true,"to":10}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int in",
		in:   "HistorySizeBytes IN (1, 2, 3)",
		sql:  "TemporalNamespaceDivision is null and history_size_bytes in (1, 2, 3)",
		es:   `{"bool":{"filter":{"terms":{"HistorySizeBytes":[1,2,3]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int between",
		in:   "HistoryLength BETWEEN 1 AND 10",
		sql:  "TemporalNamespaceDivision is null and history_length between 1 and 10",
		es:   `{"bool":{"filter":{"range":{"HistoryLength":{"from":1,"include_lower":true,"include_upper":true,"to":10}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int not between",
		in:   "HistoryLength NOT BETWEEN 1 AND 10",
		sql:  "TemporalNamespaceDivision is null and history_length not between 1 and 10",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"range":{"HistoryLength":{"from":1,"include_lower":true,"include_upper":true,"to":10}}}]}}`,
	},
	{
		name: "Int with float value",
		in:   "AliasForInt01 = 1.5",
		sql:  "TemporalNamespaceDivision is null and Int01 = 1.5",
		es:   `{"bool":{"filter":{"term":{"Int01":1.5}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Int invalid value type",
		in:   "HistoryLength = 'foo'",
		err:  `invalid expression: invalid value type for search attribute HistoryLength of type Int: "foo" (type: string)`,
	},
	{
		name: "Int unsupported operator",
		in:   "HistoryLength STARTS_WITH 1",
		err:  "operation is not supported: operator 'STARTS_WITH' not supported for Int type search attribute 'HistoryLength'",
	},
	{
		name: "Int mixed types in list",
		in:   "AliasForInt01 IN (1, 'foo')",
		err:  `invalid expression: invalid value type for search attribute AliasForInt01 of type Int: "foo" (type: string)`,
	},

	// Double type search attributes.
	{
		name: "Double equal",
		in:   "AliasForDouble01 = 1.5",
		sql:  "TemporalNamespaceDivision is null and Double01 = 1.5",
		es:   `{"bool":{"filter":{"term":{"Double01":1.5}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Double with int value",
		in:   "AliasForDouble01 = 1",
		sql:  "TemporalNamespaceDivision is null and Double01 = 1",
		es:   `{"bool":{"filter":{"term":{"Double01":1}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		// Unlike negative integers, negative floats are parsed as an unary expression.
		name: "Double negative value",
		in:   "AliasForDouble01 >= -1.5",
		sql:  "TemporalNamespaceDivision is null and Double01 >= -1.5",
		es:   `{"bool":{"filter":{"range":{"Double01":{"from":-1.5,"include_lower":true,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Double positive sign value",
		in:   "AliasForDouble01 = +1.5",
		sql:  "TemporalNamespaceDivision is null and Double01 = 1.5",
		es:   `{"bool":{"filter":{"term":{"Double01":1.5}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Double invalid value type",
		in:   "AliasForDouble01 = 'foo'",
		err:  `invalid expression: invalid value type for search attribute AliasForDouble01 of type Double: "foo" (type: string)`,
	},

	// Bool type search attributes.
	{
		name: "Bool true",
		in:   "AliasForBool01 = true",
		sql:  "TemporalNamespaceDivision is null and Bool01 = true",
		es:   `{"bool":{"filter":{"term":{"Bool01":true}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Bool false",
		in:   "TemporalSchedulePaused = false",
		sql:  "TemporalNamespaceDivision is null and TemporalSchedulePaused = false",
		es:   `{"bool":{"filter":{"term":{"TemporalSchedulePaused":false}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Bool not equal",
		in:   "AliasForBool01 != true",
		sql:  "TemporalNamespaceDivision is null and Bool01 != true",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"term":{"Bool01":true}}]}}`,
	},
	{
		name: "Bool invalid value type",
		in:   "AliasForBool01 = 'true'",
		err:  `invalid expression: invalid value type for search attribute AliasForBool01 of type Bool: "true" (type: string)`,
	},
	{
		name: "Bool range condition not supported",
		in:   "AliasForBool01 BETWEEN false AND true",
		err:  "invalid expression: cannot do range condition on search attribute 'AliasForBool01' of type Bool",
	},

	// Datetime type search attributes.
	{
		name:     "Datetime equal",
		in:       "StartTime = '2020-01-02T15:04:05Z'",
		mysql:    "TemporalNamespaceDivision is null and start_time = '2020-01-02 15:04:05'",
		postgres: "TemporalNamespaceDivision is null and start_time = '2020-01-02 15:04:05'",
		sqlite:   "TemporalNamespaceDivision is null and start_time = '2020-01-02 15:04:05+00:00'",
		es:       `{"bool":{"filter":{"term":{"StartTime":"2020-01-02T15:04:05Z"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "Datetime with nanoseconds and offset",
		in:       "StartTime > '2020-01-02T15:04:05.123456789-07:00'",
		mysql:    "TemporalNamespaceDivision is null and start_time > '2020-01-02 22:04:05.123456'",
		postgres: "TemporalNamespaceDivision is null and start_time > '2020-01-02 22:04:05.123456'",
		sqlite:   "TemporalNamespaceDivision is null and start_time > '2020-01-02 22:04:05.123456+00:00'",
		es:       `{"bool":{"filter":{"range":{"StartTime":{"from":"2020-01-02T22:04:05.123456789Z","include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "Datetime with unix nanoseconds",
		in:       "StartTime > 1577977445000000000",
		mysql:    "TemporalNamespaceDivision is null and start_time > '2020-01-02 15:04:05'",
		postgres: "TemporalNamespaceDivision is null and start_time > '2020-01-02 15:04:05'",
		sqlite:   "TemporalNamespaceDivision is null and start_time > '2020-01-02 15:04:05+00:00'",
		es:       `{"bool":{"filter":{"range":{"StartTime":{"from":"2020-01-02T15:04:05Z","include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "Datetime between",
		in:       "CloseTime BETWEEN '2020-01-02T15:04:05Z' AND '2020-01-03T15:04:05Z'",
		mysql:    "TemporalNamespaceDivision is null and close_time between '2020-01-02 15:04:05' and '2020-01-03 15:04:05'",
		postgres: "TemporalNamespaceDivision is null and close_time between '2020-01-02 15:04:05' and '2020-01-03 15:04:05'",
		sqlite:   "TemporalNamespaceDivision is null and close_time between '2020-01-02 15:04:05+00:00' and '2020-01-03 15:04:05+00:00'",
		es:       `{"bool":{"filter":{"range":{"CloseTime":{"from":"2020-01-02T15:04:05Z","include_lower":true,"include_upper":true,"to":"2020-01-03T15:04:05Z"}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Datetime is null",
		in:   "CloseTime IS NULL",
		sql:  "TemporalNamespaceDivision is null and close_time is null",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"exists":{"field":"CloseTime"}}]}}`,
	},
	{
		name: "Datetime is not null",
		in:   "CloseTime IS NOT NULL",
		sql:  "TemporalNamespaceDivision is null and close_time is not null",
		es:   `{"bool":{"filter":{"exists":{"field":"CloseTime"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "Datetime custom search attribute",
		in:       "AliasForDatetime01 <= '2020-01-02T15:04:05Z'",
		mysql:    "TemporalNamespaceDivision is null and Datetime01 <= '2020-01-02 15:04:05'",
		postgres: "TemporalNamespaceDivision is null and Datetime01 <= '2020-01-02 15:04:05'",
		sqlite:   "TemporalNamespaceDivision is null and Datetime01 <= '2020-01-02 15:04:05+00:00'",
		es:       `{"bool":{"filter":{"range":{"Datetime01":{"from":null,"include_lower":true,"include_upper":true,"to":"2020-01-02T15:04:05Z"}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Datetime invalid value",
		in:   "StartTime = 'foo'",
		err:  "invalid expression: unable to parse datetime 'foo'",
	},
	{
		name: "Datetime with bool value",
		in:   "StartTime = true",
		err:  "invalid expression: invalid value type for search attribute StartTime of type Datetime: true (type: bool)",
	},

	// ExecutionStatus search attribute.
	{
		name: "ExecutionStatus equal",
		in:   "ExecutionStatus = 'Running'",
		sql:  "TemporalNamespaceDivision is null and status = 1",
		es:   `{"bool":{"filter":{"term":{"ExecutionStatus":"Running"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionStatus equal code",
		in:   "ExecutionStatus = 1",
		sql:  "TemporalNamespaceDivision is null and status = 1",
		es:   `{"bool":{"filter":{"term":{"ExecutionStatus":"Running"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionStatus not equal",
		in:   "ExecutionStatus != 'Completed'",
		sql:  "TemporalNamespaceDivision is null and status != 2",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"term":{"ExecutionStatus":"Completed"}}]}}`,
	},
	{
		name: "ExecutionStatus in",
		in:   "ExecutionStatus IN ('Running', 'Completed')",
		sql:  "TemporalNamespaceDivision is null and status in (1, 2)",
		es:   `{"bool":{"filter":{"terms":{"ExecutionStatus":["Running","Completed"]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionStatus in code",
		in:   "ExecutionStatus IN (1, 2)",
		sql:  "TemporalNamespaceDivision is null and status in (1, 2)",
		es:   `{"bool":{"filter":{"terms":{"ExecutionStatus":["Running","Completed"]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionStatus not in",
		in:   "ExecutionStatus NOT IN ('Running', 'Completed')",
		sql:  "TemporalNamespaceDivision is null and status not in (1, 2)",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"terms":{"ExecutionStatus":["Running","Completed"]}}]}}`,
	},
	{
		name: "ExecutionStatus unspecified",
		in:   "ExecutionStatus = 'Unspecified'",
		sql:  "TemporalNamespaceDivision is null and status = 0",
		es:   `{"bool":{"filter":{"term":{"ExecutionStatus":"Unspecified"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionStatus invalid name",
		in:   "ExecutionStatus = 'Foo'",
		err:  "invalid expression: invalid ExecutionStatus value 'Foo'",
	},
	{
		name: "ExecutionStatus invalid code",
		in:   "ExecutionStatus = 42",
		err:  "invalid expression: invalid ExecutionStatus value 42",
	},
	{
		name: "ExecutionStatus invalid value type",
		in:   "ExecutionStatus = 1.5",
		err:  "invalid expression: unexpected value type float64 for search attribute ExecutionStatus",
	},
	{
		name: "ExecutionStatus invalid value in list",
		in:   "ExecutionStatus IN ('Running', 'Foo')",
		err:  "invalid expression: invalid ExecutionStatus value 'Foo'",
	},
	{
		name: "ExecutionStatus is null",
		in:   "ExecutionStatus IS NULL",
		sql:  "TemporalNamespaceDivision is null and status is null",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"exists":{"field":"ExecutionStatus"}}]}}`,
	},
	{
		// ExecutionStatus is stored as an integer in SQL databases, thus a prefix search
		// is only possible in Elasticsearch.
		name:   "ExecutionStatus starts with",
		in:     "ExecutionStatus STARTS_WITH 'Running'",
		sqlErr: `invalid expression: right-hand side of "starts_with" operator must be a literal string (got: Running)`,
		es:     `{"bool":{"filter":{"prefix":{"ExecutionStatus":"Running"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},

	// ExecutionDuration search attribute.
	{
		name: "ExecutionDuration nanoseconds",
		in:   "ExecutionDuration > 1000000",
		sql:  "TemporalNamespaceDivision is null and execution_duration > 1000000",
		es:   `{"bool":{"filter":{"range":{"ExecutionDuration":{"from":1000000,"include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionDuration golang duration",
		in:   "ExecutionDuration > '10s'",
		sql:  "TemporalNamespaceDivision is null and execution_duration > 10000000000",
		es:   `{"bool":{"filter":{"range":{"ExecutionDuration":{"from":10000000000,"include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionDuration days",
		in:   "ExecutionDuration > '2d'",
		sql:  "TemporalNamespaceDivision is null and execution_duration > 172800000000000",
		es:   `{"bool":{"filter":{"range":{"ExecutionDuration":{"from":172800000000000,"include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionDuration hh:mm:ss",
		in:   "ExecutionDuration > '00:10:30'",
		sql:  "TemporalNamespaceDivision is null and execution_duration > 630000000000",
		es:   `{"bool":{"filter":{"range":{"ExecutionDuration":{"from":630000000000,"include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionDuration between",
		in:   "ExecutionDuration BETWEEN '1s' AND '1m'",
		sql:  "TemporalNamespaceDivision is null and execution_duration between 1000000000 and 60000000000",
		es:   `{"bool":{"filter":{"range":{"ExecutionDuration":{"from":1000000000,"include_lower":true,"include_upper":true,"to":60000000000}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionDuration invalid value",
		in:   "ExecutionDuration > 'foo'",
		err:  "invalid expression: invalid duration value for search attribute ExecutionDuration: foo",
	},
	{
		name: "ExecutionDuration negative value",
		in:   "ExecutionDuration > -1000",
		sql:  "TemporalNamespaceDivision is null and execution_duration > -1000",
		es:   `{"bool":{"filter":{"range":{"ExecutionDuration":{"from":-1000,"include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ExecutionDuration with bool value",
		in:   "ExecutionDuration > true",
		err:  "invalid expression: invalid value type for search attribute ExecutionDuration of type Int: true (type: bool)",
	},

	// KeywordList type search attributes.
	{
		name:     "KeywordList equal",
		in:       "AliasForKeywordList01 = 'foo'",
		mysql:    "TemporalNamespaceDivision is null and 'foo' member of (KeywordList01)",
		postgres: "TemporalNamespaceDivision is null and KeywordList01 @> jsonb_build_array('foo')",
		sqlite:   `TemporalNamespaceDivision is null and rowid in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo")')`,
		es:       `{"bool":{"filter":{"term":{"KeywordList01":"foo"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "KeywordList not equal",
		in:       "AliasForKeywordList01 != 'foo'",
		mysql:    "TemporalNamespaceDivision is null and (not 'foo' member of (KeywordList01))",
		postgres: "TemporalNamespaceDivision is null and (not KeywordList01 @> jsonb_build_array('foo'))",
		sqlite:   `TemporalNamespaceDivision is null and rowid not in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo")')`,
		es:       `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"term":{"KeywordList01":"foo"}}]}}`,
	},
	{
		name:     "KeywordList in",
		in:       "AliasForKeywordList01 IN ('foo', 'bar')",
		mysql:    `TemporalNamespaceDivision is null and json_overlaps(KeywordList01, cast('["foo","bar"]' as json))`,
		postgres: "TemporalNamespaceDivision is null and (KeywordList01 @> jsonb_build_array('foo') or KeywordList01 @> jsonb_build_array('bar'))",
		sqlite:   `TemporalNamespaceDivision is null and rowid in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo" OR "bar")')`,
		es:       `{"bool":{"filter":{"terms":{"KeywordList01":["foo","bar"]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "KeywordList not in",
		in:       "AliasForKeywordList01 NOT IN ('foo', 'bar')",
		mysql:    `TemporalNamespaceDivision is null and (not json_overlaps(KeywordList01, cast('["foo","bar"]' as json)))`,
		postgres: "TemporalNamespaceDivision is null and (not (KeywordList01 @> jsonb_build_array('foo') or KeywordList01 @> jsonb_build_array('bar')))",
		sqlite:   `TemporalNamespaceDivision is null and rowid not in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'KeywordList01 : ("foo" OR "bar")')`,
		es:       `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"terms":{"KeywordList01":["foo","bar"]}}]}}`,
	},
	{
		name:     "KeywordList predefined search attribute",
		in:       "TemporalChangeVersion = 'foo'",
		mysql:    "TemporalNamespaceDivision is null and 'foo' member of (TemporalChangeVersion)",
		postgres: "TemporalNamespaceDivision is null and TemporalChangeVersion @> jsonb_build_array('foo')",
		sqlite:   `TemporalNamespaceDivision is null and rowid in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'TemporalChangeVersion : ("foo")')`,
		es:       `{"bool":{"filter":{"term":{"TemporalChangeVersion":"foo"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "KeywordList is null",
		in:   "AliasForKeywordList01 IS NULL",
		sql:  "TemporalNamespaceDivision is null and KeywordList01 is null",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"exists":{"field":"KeywordList01"}}]}}`,
	},
	{
		name: "KeywordList unsupported operator",
		in:   "AliasForKeywordList01 > 'foo'",
		err:  "operation is not supported: operator '>' not supported for KeywordList type search attribute 'AliasForKeywordList01'",
	},
	{
		name: "KeywordList starts with not supported",
		in:   "AliasForKeywordList01 STARTS_WITH 'foo'",
		err:  "operation is not supported: operator 'STARTS_WITH' not supported for KeywordList type search attribute 'AliasForKeywordList01'",
	},
	{
		name: "KeywordList invalid value type",
		in:   "AliasForKeywordList01 = 123",
		err:  "invalid expression: invalid value type for search attribute AliasForKeywordList01 of type KeywordList: 123 (type: int64)",
	},

	// Text type search attributes.
	{
		name:     "Text equal",
		in:       "AliasForText01 = 'foo bar'",
		mysql:    "TemporalNamespaceDivision is null and match(Text01) against ('foo bar' in natural language mode)",
		postgres: "TemporalNamespaceDivision is null and Text01 @@ 'foo | bar'::tsquery",
		sqlite:   `TemporalNamespaceDivision is null and rowid in (select rowid from executions_visibility_fts_text where executions_visibility_fts_text = 'Text01 : ("foo" OR "bar")')`,
		es:       `{"bool":{"filter":{"match":{"Text01":{"query":"foo bar"}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "Text not equal",
		in:       "AliasForText01 != 'foo bar'",
		mysql:    "TemporalNamespaceDivision is null and (not match(Text01) against ('foo bar' in natural language mode))",
		postgres: "TemporalNamespaceDivision is null and (not Text01 @@ 'foo | bar'::tsquery)",
		sqlite:   `TemporalNamespaceDivision is null and rowid not in (select rowid from executions_visibility_fts_text where executions_visibility_fts_text = 'Text01 : ("foo" OR "bar")')`,
		es:       `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"match":{"Text01":{"query":"foo bar"}}}]}}`,
	},
	{
		name: "Text empty value",
		in:   "AliasForText01 = ''",
		err:  "invalid expression: no tokens found filtering on Text type search attribute AliasForText01",
	},
	{
		name: "Text blank value",
		in:   "AliasForText01 = '   '",
		err:  "invalid expression: no tokens found filtering on Text type search attribute AliasForText01",
	},
	{
		// Only blank values are rejected: the value is passed as is to the store converters,
		// which tokenize it.
		name:     "Text value with surrounding spaces",
		in:       "AliasForText01 = '  foo bar  '",
		mysql:    "TemporalNamespaceDivision is null and match(Text01) against ('  foo bar  ' in natural language mode)",
		postgres: "TemporalNamespaceDivision is null and Text01 @@ 'foo | bar'::tsquery",
		sqlite:   `TemporalNamespaceDivision is null and rowid in (select rowid from executions_visibility_fts_text where executions_visibility_fts_text = 'Text01 : ("foo" OR "bar")')`,
		es:       `{"bool":{"filter":{"match":{"Text01":{"query":"  foo bar  "}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "Text unsupported operator",
		in:   "AliasForText01 > 'foo'",
		err:  "operation is not supported: operator '>' not supported for Text type search attribute 'AliasForText01'",
	},
	{
		name: "Text in not supported",
		in:   "AliasForText01 IN ('foo', 'bar')",
		err:  "operation is not supported: operator 'IN' not supported for Text type search attribute 'AliasForText01'",
	},
	{
		name: "Text range condition not supported",
		in:   "AliasForText01 BETWEEN 'a' AND 'b'",
		err:  "invalid expression: cannot do range condition on search attribute 'AliasForText01' of type Text",
	},
	{
		name: "Text invalid value type",
		in:   "AliasForText01 = 123",
		err:  "invalid expression: invalid value type for search attribute AliasForText01 of type Text: 123 (type: int64)",
	},

	// Search attribute name resolution.
	{
		name: "custom search attribute by field name",
		in:   "Keyword01 = 'foo'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 = 'foo'",
		es:   `{"bool":{"filter":{"term":{"Keyword01":"foo"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "custom search attribute with backticks",
		in:   "`AliasForKeyword01` = 'foo'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 = 'foo'",
		es:   `{"bool":{"filter":{"term":{"Keyword01":"foo"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "custom search attribute alias with hyphen",
		in:   "`AliasWithHyphenFor-Keyword01` = 'foo'",
		sql:  "TemporalNamespaceDivision is null and Keyword01 = 'foo'",
		es:   `{"bool":{"filter":{"term":{"Keyword01":"foo"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name:     "predefined search attribute without Temporal prefix",
		in:       "PauseInfo = 'foo'",
		mysql:    "TemporalNamespaceDivision is null and 'foo' member of (TemporalPauseInfo)",
		postgres: "TemporalNamespaceDivision is null and TemporalPauseInfo @> jsonb_build_array('foo')",
		sqlite:   `TemporalNamespaceDivision is null and rowid in (select rowid from executions_visibility_fts_keyword_list where executions_visibility_fts_keyword_list = 'TemporalPauseInfo : ("foo")')`,
		es:       `{"bool":{"filter":{"term":{"TemporalPauseInfo":"foo"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "system search attribute with Temporal prefix",
		in:   "TemporalHistoryLength > 10",
		sql:  "TemporalNamespaceDivision is null and history_length > 10",
		es:   `{"bool":{"filter":{"range":{"HistoryLength":{"from":10,"include_lower":false,"include_upper":true,"to":null}}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "ScheduleId is mapped to WorkflowId",
		in:   "ScheduleId = 'sched-id'",
		sql:  "TemporalNamespaceDivision is null and workflow_id = 'sched-id'",
		es:   `{"bool":{"filter":{"term":{"WorkflowId":"sched-id"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "TemporalScheduleId is mapped to WorkflowId",
		in:   "TemporalScheduleId = 'sched-id'",
		sql:  "TemporalNamespaceDivision is null and workflow_id = 'sched-id'",
		es:   `{"bool":{"filter":{"term":{"WorkflowId":"sched-id"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "unknown search attribute",
		in:   "Foo = 'bar'",
		err:  "invalid expression: column name 'Foo' is not a valid search attribute",
	},
	{
		name: "reserved field name is not a search attribute",
		in:   "NamespaceId = 'foo'",
		err:  "invalid expression: column name 'NamespaceId' is not a valid search attribute",
	},

	// Namespace division.
	{
		name: "explicit namespace division",
		in:   "TemporalNamespaceDivision = 'foo'",
		sql:  "TemporalNamespaceDivision = 'foo'",
		es:   `{"term":{"TemporalNamespaceDivision":"foo"}}`,
	},
	{
		name: "explicit namespace division is null",
		in:   "TemporalNamespaceDivision IS NULL",
		sql:  "TemporalNamespaceDivision is null",
		es:   `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "match any namespace division",
		in:   "TemporalNamespaceDivision IS NULL OR TemporalNamespaceDivision IS NOT NULL",
		sql:  "(TemporalNamespaceDivision is null or TemporalNamespaceDivision is not null)",
		es:   `{"bool":{"minimum_should_match":"1","should":[{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}},{"exists":{"field":"TemporalNamespaceDivision"}}]}}`,
	},
	{
		name: "namespace division in complex query",
		in:   "WorkflowId = 'wid' AND (TemporalNamespaceDivision = 'foo' OR TemporalNamespaceDivision = 'bar')",
		sql:  "(workflow_id = 'wid' and (TemporalNamespaceDivision = 'foo' or TemporalNamespaceDivision = 'bar'))",
		es:   `{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"bool":{"minimum_should_match":"1","should":[{"term":{"TemporalNamespaceDivision":"foo"}},{"term":{"TemporalNamespaceDivision":"bar"}}]}}]}}`,
	},

	// Logical operators.
	{
		name: "and expression",
		in:   "WorkflowId = 'wid' AND ExecutionStatus = 'Running'",
		sql:  "TemporalNamespaceDivision is null and (workflow_id = 'wid' and status = 1)",
		es:   `{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"term":{"ExecutionStatus":"Running"}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "or expression",
		in:   "WorkflowId = 'wid' OR RunId = 'rid'",
		sql:  "TemporalNamespaceDivision is null and (workflow_id = 'wid' or run_id = 'rid')",
		es:   `{"bool":{"filter":{"bool":{"minimum_should_match":"1","should":[{"term":{"WorkflowId":"wid"}},{"term":{"RunId":"rid"}}]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "multiple and expressions",
		in:   "WorkflowId = 'wid' AND RunId = 'rid' AND WorkflowType = 'wtype'",
		sql:  "TemporalNamespaceDivision is null and ((workflow_id = 'wid' and run_id = 'rid') and workflow_type_name = 'wtype')",
		es:   `{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"term":{"RunId":"rid"}},{"term":{"WorkflowType":"wtype"}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "multiple or expressions",
		in:   "WorkflowId = 'wid' OR RunId = 'rid' OR WorkflowType = 'wtype'",
		sql:  "TemporalNamespaceDivision is null and (workflow_id = 'wid' or run_id = 'rid' or workflow_type_name = 'wtype')",
		es:   `{"bool":{"filter":{"bool":{"minimum_should_match":"1","should":[{"term":{"WorkflowId":"wid"}},{"term":{"RunId":"rid"}},{"term":{"WorkflowType":"wtype"}}]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "and or precedence",
		in:   "WorkflowId = 'wid' AND RunId = 'rid' OR WorkflowType = 'wtype'",
		sql:  "TemporalNamespaceDivision is null and (workflow_id = 'wid' and run_id = 'rid' or workflow_type_name = 'wtype')",
		es:   `{"bool":{"filter":{"bool":{"minimum_should_match":"1","should":[{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"term":{"RunId":"rid"}}]}},{"term":{"WorkflowType":"wtype"}}]}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "parenthesized expression",
		in:   "WorkflowId = 'wid' AND (RunId = 'rid' OR WorkflowType = 'wtype')",
		sql:  "TemporalNamespaceDivision is null and (workflow_id = 'wid' and (run_id = 'rid' or workflow_type_name = 'wtype'))",
		es:   `{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"bool":{"minimum_should_match":"1","should":[{"term":{"RunId":"rid"}},{"term":{"WorkflowType":"wtype"}}]}}],"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "nested parenthesized expression",
		in:   "((WorkflowId = 'wid'))",
		sql:  "TemporalNamespaceDivision is null and workflow_id = 'wid'",
		es:   `{"bool":{"filter":{"term":{"WorkflowId":"wid"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
	},
	{
		name: "not expression",
		in:   "NOT WorkflowId = 'wid'",
		sql:  "TemporalNamespaceDivision is null and (not workflow_id = 'wid')",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"term":{"WorkflowId":"wid"}}]}}`,
	},
	{
		name: "not parenthesized and expression",
		in:   "NOT (WorkflowId = 'wid' AND RunId = 'rid')",
		sql:  "TemporalNamespaceDivision is null and (not (workflow_id = 'wid' and run_id = 'rid'))",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"bool":{"filter":[{"term":{"WorkflowId":"wid"}},{"term":{"RunId":"rid"}}]}}]}}`,
	},
	{
		name: "not parenthesized or expression",
		in:   "NOT (WorkflowId = 'wid' OR RunId = 'rid')",
		sql:  "TemporalNamespaceDivision is null and (not (workflow_id = 'wid' or run_id = 'rid'))",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"term":{"WorkflowId":"wid"}},{"term":{"RunId":"rid"}}]}}`,
	},
	{
		name: "not is null expression",
		in:   "NOT CloseTime IS NULL",
		sql:  "TemporalNamespaceDivision is null and (not close_time is null)",
		es:   `{"bool":{"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}`,
	},
	{
		name: "complex query",
		in: "WorkflowType = 'wtype' AND ExecutionStatus IN ('Running', 'Failed') " +
			"AND (StartTime > '2020-01-02T15:04:05Z' OR CloseTime IS NULL) " +
			"AND NOT AliasForKeyword01 STARTS_WITH 'foo'",
		mysql: "TemporalNamespaceDivision is null and " +
			"(((workflow_type_name = 'wtype' and status in (1, 3)) and " +
			"(start_time > '2020-01-02 15:04:05' or close_time is null)) and " +
			"(not Keyword01 like 'foo%' escape '!'))",
		postgres: "TemporalNamespaceDivision is null and " +
			"(((workflow_type_name = 'wtype' and status in (1, 3)) and " +
			"(start_time > '2020-01-02 15:04:05' or close_time is null)) and " +
			"(not Keyword01 like 'foo%' escape '!'))",
		sqlite: "TemporalNamespaceDivision is null and " +
			"(((workflow_type_name = 'wtype' and status in (1, 3)) and " +
			"(start_time > '2020-01-02 15:04:05+00:00' or close_time is null)) and " +
			"(not Keyword01 like 'foo%' escape '!'))",
		es: `{"bool":{"filter":[{"term":{"WorkflowType":"wtype"}},` +
			`{"terms":{"ExecutionStatus":["Running","Failed"]}},` +
			`{"bool":{"minimum_should_match":"1","should":[` +
			`{"range":{"StartTime":{"from":"2020-01-02T15:04:05Z","include_lower":false,"include_upper":true,"to":null}}},` +
			`{"bool":{"must_not":{"exists":{"field":"CloseTime"}}}}]}}],` +
			`"must_not":[{"exists":{"field":"TemporalNamespaceDivision"}},{"prefix":{"Keyword01":"foo"}}]}}`,
	},
	{
		name: "error inside and expression",
		in:   "WorkflowId = 'wid' AND Foo = 'bar'",
		err:  "invalid expression: column name 'Foo' is not a valid search attribute",
	},
	{
		name: "error inside or expression",
		in:   "Foo = 'bar' OR WorkflowId = 'wid'",
		err:  "invalid expression: column name 'Foo' is not a valid search attribute",
	},
	{
		name: "error inside not expression",
		in:   "NOT Foo = 'bar'",
		err:  "invalid expression: column name 'Foo' is not a valid search attribute",
	},

	// GROUP BY clause.
	{
		name:    "group by execution status",
		in:      "GROUP BY ExecutionStatus",
		sql:     "TemporalNamespaceDivision is null",
		es:      `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
		groupBy: []string{"ExecutionStatus"},
	},
	{
		name:    "group by with where clause",
		in:      "WorkflowType = 'wtype' GROUP BY ExecutionStatus",
		sql:     "TemporalNamespaceDivision is null and workflow_type_name = 'wtype'",
		es:      `{"bool":{"filter":{"term":{"WorkflowType":"wtype"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
		groupBy: []string{"ExecutionStatus"},
	},
	{
		name:    "group by namespace division",
		in:      "GROUP BY TemporalNamespaceDivision",
		groupBy: []string{"TemporalNamespaceDivision"},
	},
	{
		name: "group by unsupported search attribute",
		in:   "GROUP BY WorkflowId",
		err:  "operation is not supported: 'GROUP BY' clause is not supported for search attribute WorkflowId",
	},
	{
		name: "group by multiple search attributes",
		in:   "GROUP BY ExecutionStatus, TemporalNamespaceDivision",
		err:  "operation is not supported: 'GROUP BY' clause supports only a single field",
	},
	{
		name: "group by unknown search attribute",
		in:   "GROUP BY Foo",
		err:  "invalid expression: column name 'Foo' is not a valid search attribute",
	},

	// ORDER BY clause.
	{
		name:    "order by",
		in:      "ORDER BY StartTime",
		sql:     "TemporalNamespaceDivision is null",
		es:      `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
		orderBy: "order by start_time asc",
	},
	{
		name:    "order by desc",
		in:      "ORDER BY CloseTime DESC",
		sql:     "TemporalNamespaceDivision is null",
		es:      `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
		orderBy: "order by close_time desc",
	},
	{
		name:    "order by multiple search attributes",
		in:      "ORDER BY StartTime DESC, RunId ASC",
		sql:     "TemporalNamespaceDivision is null",
		es:      `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
		orderBy: "order by start_time desc, run_id asc",
	},
	{
		name:    "order by with where clause",
		in:      "WorkflowId = 'wid' ORDER BY StartTime DESC",
		sql:     "TemporalNamespaceDivision is null and workflow_id = 'wid'",
		es:      `{"bool":{"filter":{"term":{"WorkflowId":"wid"}},"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
		orderBy: "order by start_time desc",
	},
	{
		name:    "order by custom search attribute",
		in:      "ORDER BY AliasForKeyword01",
		sql:     "TemporalNamespaceDivision is null",
		es:      `{"bool":{"must_not":{"exists":{"field":"TemporalNamespaceDivision"}}}}`,
		orderBy: "order by Keyword01 asc",
	},
	{
		name: "order by text search attribute",
		in:   "ORDER BY AliasForText01",
		err:  "operation is not supported: unable to sort by search attribute type Text",
	},
	{
		name: "order by unknown search attribute",
		in:   "ORDER BY Foo",
		err:  "invalid expression: column name 'Foo' is not a valid search attribute",
	},

	// Malformed and unsupported queries.
	{
		name: "malformed query",
		in:   "this is not a query",
		err:  "malformed SQL query: syntax error at position 41 near 'a'",
	},
	{
		name: "missing value",
		in:   "WorkflowId =",
		err:  "malformed SQL query: syntax error at position 40",
	},
	{
		name: "incomplete expression",
		in:   "WorkflowId",
		err:  "invalid expression: incomplete expression",
	},
	{
		name: "column name on right hand side",
		in:   "WorkflowId = RunId",
		err:  `operation is not supported: column name on the right side of comparison expression (did you forget to quote "RunId"?)`,
	},
	{
		name: "function expression",
		in:   "length(WorkflowId)",
		err:  "operation is not supported: function expression",
	},
	{
		name: "function expression on left hand side",
		in:   "length(WorkflowId) = 5",
		err:  "invalid expression: must be a column name but was *sqlparser.FuncExpr",
	},
	{
		name: "function expression on right hand side",
		in:   "WorkflowId = length('foo')",
		err:  "operation is not supported: nested func",
	},
	{
		name: "limit clause",
		in:   "WorkflowId = 'wid' LIMIT 10",
		err:  "operation is not supported: 'LIMIT' clause",
	},
	{
		name: "subquery",
		in:   "WorkflowId IN (SELECT * FROM table1)",
		err:  "invalid expression: unexpected value type *sqlparser.Subquery",
	},
	{
		name: "arithmetic expression",
		in:   "HistoryLength = 1 + 1",
		err:  "invalid expression: unexpected value type *sqlparser.BinaryExpr",
	},
	{
		name: "unsupported unary operator",
		in:   "AliasForInt01 = ~1",
		err:  `operation is not supported: unary operator "~"`,
	},
}

func runQueryConverterTest[T any](
	t *testing.T,
	store string,
	newQueryConverter func() *query.QueryConverter[T],
	serialize func(T) (string, error),
) {
	for _, tc := range queryConverterTestCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			expectedOut, expectedErr := tc.expected(store)
			// The query converter is stateful (e.g. it tracks if the query filtered by
			// namespace division), thus each test case requires a new instance.
			queryParams, err := newQueryConverter().Convert(tc.in)
			if expectedErr != "" {
				r.Error(err)
				r.EqualError(err, expectedErr)
				var converterErr *query.ConverterError
				r.ErrorAs(err, &converterErr)
				r.Nil(queryParams)
				return
			}
			r.NoError(err)
			out, err := serialize(queryParams.QueryExpr)
			r.NoError(err)
			r.Equal(expectedOut, out)

			var groupBy []string
			for _, col := range queryParams.GroupBy {
				groupBy = append(groupBy, col.FieldName)
			}
			r.Equal(tc.groupBy, groupBy)
			r.Equal(tc.orderBy, strings.TrimSpace(sqlparser.String(queryParams.OrderBy)))
		})
	}
}

func serializeSQLQuery(expr sqlparser.Expr) (string, error) {
	if expr == nil {
		return "", nil
	}
	return sqlparser.String(expr), nil
}

func serializeESQuery(q elastic.Query) (string, error) {
	if q == nil {
		return "", nil
	}
	source, err := q.Source()
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(source)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func TestSQLQueryConverter(t *testing.T) {
	t.Parallel()
	for _, plugin := range sqlPlugins {
		t.Run(plugin, func(t *testing.T) {
			t.Parallel()
			pluginVisQC, err := sql.GetPluginVisibilityQueryConverter(plugin)
			require.NoError(t, err)
			runQueryConverterTest(
				t,
				plugin,
				func() *query.QueryConverter[sqlparser.Expr] {
					return query.NewQueryConverter(
						&vissql.SQLQueryConverter{VisibilityQueryConverter: pluginVisQC},
						testNamespaceName,
						searchattribute.TestNameTypeMap(),
						&searchattribute.TestMapper{},
						nil, // metricsHandler
						log.NewNoopLogger(),
					)
				},
				serializeSQLQuery,
			)
		})
	}
}

func TestElasticsearchQueryConverter(t *testing.T) {
	t.Parallel()
	runQueryConverterTest(
		t,
		esStore,
		func() *query.QueryConverter[elastic.Query] {
			return elasticsearch.NewQueryConverter(
				testNamespaceName,
				searchattribute.TestNameTypeMap(),
				&searchattribute.TestMapper{},
				nil, // metricsHandler
				log.NewNoopLogger(),
			)
		},
		serializeESQuery,
	)
}
