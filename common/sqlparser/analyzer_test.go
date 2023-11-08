/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestPreview(t *testing.T) {
	testcases := []struct {
		sql  string
		want int
	}{
		{"select ...", StmtSelect},
		{"    select ...", StmtSelect},
		{"(select ...", StmtSelect},
		{"insert ...", StmtInsert},
		{"replace ....", StmtReplace},
		{"   update ...", StmtUpdate},
		{"Update", StmtUpdate},
		{"UPDATE ...", StmtUpdate},
		{"\n\t    delete ...", StmtDelete},
		{"", StmtUnknown},
		{" ", StmtUnknown},
		{"begin", StmtBegin},
		{" begin", StmtBegin},
		{" begin ", StmtBegin},
		{"\n\t begin ", StmtBegin},
		{"... begin ", StmtUnknown},
		{"begin ...", StmtUnknown},
		{"begin /* ... */", StmtBegin},
		{"begin /* ... *//*test*/", StmtBegin},
		{"start transaction", StmtBegin},
		{"commit", StmtCommit},
		{"commit /*...*/", StmtCommit},
		{"rollback", StmtRollback},
		{"rollback /*...*/", StmtRollback},
		{"create", StmtDDL},
		{"alter", StmtDDL},
		{"rename", StmtDDL},
		{"drop", StmtDDL},
		{"set", StmtSet},
		{"show", StmtShow},
		{"use", StmtUse},
		{"analyze", StmtOther},
		{"describe", StmtOther},
		{"desc", StmtOther},
		{"explain", StmtOther},
		{"repair", StmtOther},
		{"optimize", StmtOther},
		{"truncate", StmtDDL},
		{"unknown", StmtUnknown},

		{"/* leading comment */ select ...", StmtSelect},
		{"/* leading comment */ (select ...", StmtSelect},
		{"/* leading comment */ /* leading comment 2 */ select ...", StmtSelect},
		{"/*! MySQL-specific comment */", StmtComment},
		{"/*!50708 MySQL-version comment */", StmtComment},
		{"-- leading single line comment \n select ...", StmtSelect},
		{"-- leading single line comment \n -- leading single line comment 2\n select ...", StmtSelect},

		{"/* leading comment no end select ...", StmtUnknown},
		{"-- leading single line comment no end select ...", StmtUnknown},
	}
	for _, tcase := range testcases {
		if got := Preview(tcase.sql); got != tcase.want {
			t.Errorf("Preview(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestIsDML(t *testing.T) {
	testcases := []struct {
		sql  string
		want bool
	}{
		{"   update ...", true},
		{"Update", true},
		{"UPDATE ...", true},
		{"\n\t    delete ...", true},
		{"insert ...", true},
		{"replace ...", true},
		{"select ...", false},
		{"    select ...", false},
		{"", false},
		{" ", false},
	}
	for _, tcase := range testcases {
		if got := IsDML(tcase.sql); got != tcase.want {
			t.Errorf("IsDML(%s): %v, want %v", tcase.sql, got, tcase.want)
		}
	}
}

func TestGetTableName(t *testing.T) {
	testcases := []struct {
		in, out string
	}{{
		in:  "select * from t",
		out: "t",
	}, {
		in:  "select * from t.t",
		out: "",
	}, {
		in:  "select * from (select * from t) as tt",
		out: "",
	}}

	for _, tc := range testcases {
		tree, err := Parse(tc.in)
		if err != nil {
			t.Error(err)
			continue
		}
		out := GetTableName(tree.(*Select).From[0].(*AliasedTableExpr).Expr)
		if out.String() != tc.out {
			t.Errorf("GetTableName('%s'): %s, want %s", tc.in, out, tc.out)
		}
	}
}

func TestIsColName(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  &ColName{},
		out: true,
	}, {
		in: newHexVal(""),
	}}
	for _, tc := range testcases {
		out := IsColName(tc.in)
		if out != tc.out {
			t.Errorf("IsColName(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsValue(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  newStrVal("aa"),
		out: true,
	}, {
		in:  newHexVal("3131"),
		out: true,
	}, {
		in:  newIntVal("1"),
		out: true,
	}, {
		in:  newValArg(":a"),
		out: true,
	}, {
		in:  &NullVal{},
		out: false,
	}}
	for _, tc := range testcases {
		out := IsValue(tc.in)
		if out != tc.out {
			t.Errorf("IsValue(%T): %v, want %v", tc.in, out, tc.out)
		}
		if tc.out {
			// NewPlanValue should not fail for valid values.
			if _, err := NewPlanValue(tc.in); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestIsNull(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  &NullVal{},
		out: true,
	}, {
		in: newStrVal(""),
	}}
	for _, tc := range testcases {
		out := IsNull(tc.in)
		if out != tc.out {
			t.Errorf("IsNull(%T): %v, want %v", tc.in, out, tc.out)
		}
	}
}

func TestIsSimpleTuple(t *testing.T) {
	testcases := []struct {
		in  Expr
		out bool
	}{{
		in:  ValTuple{newStrVal("aa")},
		out: true,
	}, {
		in: ValTuple{&ColName{}},
	}, {
		in:  ListArg("::a"),
		out: true,
	}, {
		in: &ColName{},
	}}
	for _, tc := range testcases {
		out := IsSimpleTuple(tc.in)
		if out != tc.out {
			t.Errorf("IsSimpleTuple(%T): %v, want %v", tc.in, out, tc.out)
		}
		if tc.out {
			// NewPlanValue should not fail for valid tuples.
			if _, err := NewPlanValue(tc.in); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestNewPlanValue(t *testing.T) {
	tcases := []struct {
		in  Expr
		out sqltypes.PlanValue
		err string
	}{{
		in: &SQLVal{
			Type: ValArg,
			Val:  []byte(":valarg"),
		},
		out: sqltypes.PlanValue{Key: "valarg"},
	}, {
		in: &SQLVal{
			Type: IntVal,
			Val:  []byte("10"),
		},
		out: sqltypes.PlanValue{Value: sqltypes.NewInt64(10)},
	}, {
		in: &SQLVal{
			Type: IntVal,
			Val:  []byte("1111111111111111111111111111111111111111"),
		},
		err: "value out of range",
	}, {
		in: &SQLVal{
			Type: StrVal,
			Val:  []byte("strval"),
		},
		out: sqltypes.PlanValue{Value: sqltypes.NewVarBinary("strval")},
	}, {
		in: &SQLVal{
			Type: HexVal,
			Val:  []byte("3131"),
		},
		out: sqltypes.PlanValue{Value: sqltypes.NewVarBinary("11")},
	}, {
		in: &SQLVal{
			Type: HexVal,
			Val:  []byte("313"),
		},
		err: "odd length hex string",
	}, {
		in:  ListArg("::list"),
		out: sqltypes.PlanValue{ListKey: "list"},
	}, {
		in: ValTuple{
			&SQLVal{
				Type: ValArg,
				Val:  []byte(":valarg"),
			},
			&SQLVal{
				Type: StrVal,
				Val:  []byte("strval"),
			},
		},
		out: sqltypes.PlanValue{
			Values: []sqltypes.PlanValue{{
				Key: "valarg",
			}, {
				Value: sqltypes.NewVarBinary("strval"),
			}},
		},
	}, {
		in: ValTuple{
			&ParenExpr{Expr: &SQLVal{
				Type: ValArg,
				Val:  []byte(":valarg"),
			}},
		},
		err: "expression is too complex",
	}, {
		in: ValTuple{
			ListArg("::list"),
		},
		err: "unsupported: nested lists",
	}, {
		in:  &NullVal{},
		out: sqltypes.PlanValue{},
	}, {
		in: &ParenExpr{Expr: &SQLVal{
			Type: ValArg,
			Val:  []byte(":valarg"),
		}},
		err: "expression is too complex",
	}}
	for _, tc := range tcases {
		got, err := NewPlanValue(tc.in)
		if err != nil {
			if !strings.Contains(err.Error(), tc.err) {
				t.Errorf("NewPlanValue(%s) error: %v, want '%s'", String(tc.in), err, tc.err)
			}
			continue
		}
		if tc.err != "" {
			t.Errorf("NewPlanValue(%s) error: nil, want '%s'", String(tc.in), tc.err)
			continue
		}
		if !reflect.DeepEqual(got, tc.out) {
			t.Errorf("NewPlanValue(%s): %v, want %v", String(tc.in), got, tc.out)
		}
	}
}

func TestStringIn(t *testing.T) {
	testcases := []struct {
		in1 string
		in2 []string
		out bool
	}{{
		in1: "v1",
		in2: []string{"v1", "v2"},
		out: true,
	}, {
		in1: "v0",
		in2: []string{"v1", "v2"},
	}}
	for _, tc := range testcases {
		out := StringIn(tc.in1, tc.in2...)
		if out != tc.out {
			t.Errorf("StringIn(%v,%v): %#v, want %#v", tc.in1, tc.in2, out, tc.out)
		}
	}
}

func TestExtractSetValues(t *testing.T) {
	testcases := []struct {
		sql   string
		out   map[SetKey]interface{}
		scope string
		err   string
	}{{
		sql: "invalid",
		err: "syntax error at position 8 near 'invalid'",
	}, {
		sql: "select * from t",
		err: "ast did not yield *sqlparser.Set: *sqlparser.Select",
	}, {
		sql: "set autocommit=1+1",
		err: "invalid syntax: 1 + 1",
	}, {
		sql: "set transaction_mode='single'",
		out: map[SetKey]interface{}{{Key: "transaction_mode", Scope: "session"}: "single"},
	}, {
		sql: "set autocommit=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "set autocommit=true",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "set autocommit=false",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(0)},
	}, {
		sql: "set autocommit=on",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: "on"},
	}, {
		sql: "set autocommit=off",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: "off"},
	}, {
		sql: "set @@global.autocommit=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "global"}: int64(1)},
	}, {
		sql: "set @@global.autocommit=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "global"}: int64(1)},
	}, {
		sql: "set @@session.autocommit=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "set @@session.`autocommit`=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "set @@session.'autocommit'=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "set @@session.\"autocommit\"=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "set @@session.'\"autocommit'=1",
		out: map[SetKey]interface{}{{Key: "\"autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "set @@session.`autocommit'`=1",
		out: map[SetKey]interface{}{{Key: "autocommit'", Scope: "session"}: int64(1)},
	}, {
		sql: "set AUTOCOMMIT=1",
		out: map[SetKey]interface{}{{Key: "autocommit", Scope: "session"}: int64(1)},
	}, {
		sql: "SET character_set_results = NULL",
		out: map[SetKey]interface{}{{Key: "character_set_results", Scope: "session"}: nil},
	}, {
		sql: "SET foo = 0x1234",
		err: "invalid value type: 0x1234",
	}, {
		sql: "SET names utf8",
		out: map[SetKey]interface{}{{Key: "names", Scope: "session"}: "utf8"},
	}, {
		sql: "SET names ascii collate ascii_bin",
		out: map[SetKey]interface{}{{Key: "names", Scope: "session"}: "ascii"},
	}, {
		sql: "SET charset default",
		out: map[SetKey]interface{}{{Key: "charset", Scope: "session"}: "default"},
	}, {
		sql: "SET character set ascii",
		out: map[SetKey]interface{}{{Key: "charset", Scope: "session"}: "ascii"},
	}, {
		sql:   "SET SESSION wait_timeout = 3600",
		out:   map[SetKey]interface{}{{Key: "wait_timeout", Scope: "session"}: int64(3600)},
		scope: "session",
	}, {
		sql:   "SET GLOBAL wait_timeout = 3600",
		out:   map[SetKey]interface{}{{Key: "wait_timeout", Scope: "session"}: int64(3600)},
		scope: "global",
	}, {
		sql:   "set session transaction isolation level repeatable read",
		out:   map[SetKey]interface{}{{Key: "tx_isolation", Scope: "session"}: "repeatable read"},
		scope: "session",
	}, {
		sql:   "set session transaction isolation level read committed",
		out:   map[SetKey]interface{}{{Key: "tx_isolation", Scope: "session"}: "read committed"},
		scope: "session",
	}, {
		sql:   "set session transaction isolation level read uncommitted",
		out:   map[SetKey]interface{}{{Key: "tx_isolation", Scope: "session"}: "read uncommitted"},
		scope: "session",
	}, {
		sql:   "set session transaction isolation level serializable",
		out:   map[SetKey]interface{}{{Key: "tx_isolation", Scope: "session"}: "serializable"},
		scope: "session",
	}, {
		sql:   "set session tx_read_only = 0",
		out:   map[SetKey]interface{}{{Key: "tx_read_only", Scope: "session"}: int64(0)},
		scope: "session",
	}, {
		sql:   "set session tx_read_only = 1",
		out:   map[SetKey]interface{}{{Key: "tx_read_only", Scope: "session"}: int64(1)},
		scope: "session",
	}, {
		sql:   "set session sql_safe_updates = 0",
		out:   map[SetKey]interface{}{{Key: "sql_safe_updates", Scope: "session"}: int64(0)},
		scope: "session",
	}, {
		sql:   "set session sql_safe_updates = 1",
		out:   map[SetKey]interface{}{{Key: "sql_safe_updates", Scope: "session"}: int64(1)},
		scope: "session",
	}}
	for _, tcase := range testcases {
		out, _, err := ExtractSetValues(tcase.sql)
		if tcase.err != "" {
			if err == nil || err.Error() != tcase.err {
				t.Errorf("ExtractSetValues(%s): %v, want '%s'", tcase.sql, err, tcase.err)
			}
		} else if err != nil {
			t.Errorf("ExtractSetValues(%s): %v, want no error", tcase.sql, err)
		}
		if !reflect.DeepEqual(out, tcase.out) {
			t.Errorf("ExtractSetValues(%s): %v, want '%v'", tcase.sql, out, tcase.out)
		}
	}
}

func newStrVal(in string) *SQLVal {
	return NewStrVal([]byte(in))
}

func newIntVal(in string) *SQLVal {
	return NewIntVal([]byte(in))
}

func newHexVal(in string) *SQLVal {
	return NewHexVal([]byte(in))
}

func newValArg(in string) *SQLVal {
	return NewValArg([]byte(in))
}
