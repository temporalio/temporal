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
	"fmt"
	"reflect"
	"testing"

	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestNormalize(t *testing.T) {
	prefix := "bv"
	testcases := []struct {
		in      string
		outstmt string
		outbv   map[string]*querypb.BindVariable
	}{{
		// str val
		in:      "select * from t where v1 = 'aa'",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BytesBindVariable([]byte("aa")),
		},
	}, {
		// int val
		in:      "select * from t where v1 = 1",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}, {
		// float val
		in:      "select * from t where v1 = 1.2",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Float64BindVariable(1.2),
		},
	}, {
		// multiple vals
		in:      "select * from t where v1 = 1.2 and v2 = 2",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Float64BindVariable(1.2),
			"bv2": sqltypes.Int64BindVariable(2),
		},
	}, {
		// bv collision
		in:      "select * from t where v1 = :bv1 and v2 = 1",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv2": sqltypes.Int64BindVariable(1),
		},
	}, {
		// val reuse
		in:      "select * from t where v1 = 1 and v2 = 1",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}, {
		// ints and strings are different
		in:      "select * from t where v1 = 1 and v2 = '1'",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.BytesBindVariable([]byte("1")),
		},
	}, {
		// val should not be reused for non-select statements
		in:      "insert into a values(1, 1)",
		outstmt: "insert into a values (:bv1, :bv2)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
			"bv2": sqltypes.Int64BindVariable(1),
		},
	}, {
		// val should be reused only in subqueries of DMLs
		in:      "update a set v1=(select 5 from t), v2=5, v3=(select 5 from t), v4=5",
		outstmt: "update a set v1 = (select :bv1 from t), v2 = :bv2, v3 = (select :bv1 from t), v4 = :bv3",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(5),
			"bv2": sqltypes.Int64BindVariable(5),
			"bv3": sqltypes.Int64BindVariable(5),
		},
	}, {
		// list vars should work for DMLs also
		in:      "update a set v1=5 where v2 in (1, 4, 5)",
		outstmt: "update a set v1 = :bv1 where v2 in ::bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(5),
			"bv2": sqltypes.TestBindVariable([]interface{}{1, 4, 5}),
		},
	}, {
		// Hex value does not convert
		in:      "select * from t where v1 = 0x1234",
		outstmt: "select * from t where v1 = 0x1234",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// Hex value does not convert for DMLs
		in:      "update a set v1 = 0x1234",
		outstmt: "update a set v1 = 0x1234",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// Values up to len 256 will reuse.
		in:      fmt.Sprintf("select * from t where v1 = '%256s' and v2 = '%256s'", "a", "a"),
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BytesBindVariable([]byte(fmt.Sprintf("%256s", "a"))),
		},
	}, {
		// Values greater than len 256 will not reuse.
		in:      fmt.Sprintf("select * from t where v1 = '%257s' and v2 = '%257s'", "b", "b"),
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.BytesBindVariable([]byte(fmt.Sprintf("%257s", "b"))),
			"bv2": sqltypes.BytesBindVariable([]byte(fmt.Sprintf("%257s", "b"))),
		},
	}, {
		// bad int
		in:      "select * from t where v1 = 12345678901234567890",
		outstmt: "select * from t where v1 = 12345678901234567890",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// comparison with no vals
		in:      "select * from t where v1 = v2",
		outstmt: "select * from t where v1 = v2",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// IN clause with existing bv
		in:      "select * from t where v1 in ::list",
		outstmt: "select * from t where v1 in ::list",
		outbv:   map[string]*querypb.BindVariable{},
	}, {
		// IN clause with non-val values
		in:      "select * from t where v1 in (1, a)",
		outstmt: "select * from t where v1 in (:bv1, a)",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.Int64BindVariable(1),
		},
	}, {
		// IN clause with vals
		in:      "select * from t where v1 in (1, '2')",
		outstmt: "select * from t where v1 in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.TestBindVariable([]interface{}{1, []byte("2")}),
		},
	}, {
		// NOT IN clause
		in:      "select * from t where v1 not in (1, '2')",
		outstmt: "select * from t where v1 not in ::bv1",
		outbv: map[string]*querypb.BindVariable{
			"bv1": sqltypes.TestBindVariable([]interface{}{1, []byte("2")}),
		},
	}}
	for _, tc := range testcases {
		stmt, err := Parse(tc.in)
		if err != nil {
			t.Error(err)
			continue
		}
		bv := make(map[string]*querypb.BindVariable)
		Normalize(stmt, bv, prefix)
		outstmt := String(stmt)
		if outstmt != tc.outstmt {
			t.Errorf("Query:\n%s:\n%s, want\n%s", tc.in, outstmt, tc.outstmt)
		}
		if !reflect.DeepEqual(tc.outbv, bv) {
			t.Errorf("Query:\n%s:\n%v, want\n%v", tc.in, bv, tc.outbv)
		}
	}
}

func TestGetBindVars(t *testing.T) {
	stmt, err := Parse("select * from t where :v1 = :v2 and :v2 = :v3 and :v4 in ::v5")
	if err != nil {
		t.Fatal(err)
	}
	got := GetBindvars(stmt)
	want := map[string]struct{}{
		"v1": {},
		"v2": {},
		"v3": {},
		"v4": {},
		"v5": {},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetBindVars: %v, want: %v", got, want)
	}
}
