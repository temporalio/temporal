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

package sqltypes

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xwb1989/sqlparser/dependency/querypb"
)

func TestPlanValueIsNull(t *testing.T) {
	tcases := []struct {
		in  PlanValue
		out bool
	}{{
		in:  PlanValue{},
		out: true,
	}, {
		in:  PlanValue{Key: "aa"},
		out: false,
	}, {
		in:  PlanValue{Value: NewVarBinary("aa")},
		out: false,
	}, {
		in:  PlanValue{ListKey: "aa"},
		out: false,
	}, {
		in:  PlanValue{Values: []PlanValue{}},
		out: false,
	}}
	for _, tc := range tcases {
		got := tc.in.IsNull()
		if got != tc.out {
			t.Errorf("IsNull(%v): %v, want %v", tc.in, got, tc.out)
		}
	}
}

func TestPlanValueIsList(t *testing.T) {
	tcases := []struct {
		in  PlanValue
		out bool
	}{{
		in:  PlanValue{},
		out: false,
	}, {
		in:  PlanValue{Key: "aa"},
		out: false,
	}, {
		in:  PlanValue{Value: NewVarBinary("aa")},
		out: false,
	}, {
		in:  PlanValue{ListKey: "aa"},
		out: true,
	}, {
		in:  PlanValue{Values: []PlanValue{}},
		out: true,
	}}
	for _, tc := range tcases {
		got := tc.in.IsList()
		if got != tc.out {
			t.Errorf("IsList(%v): %v, want %v", tc.in, got, tc.out)
		}
	}
}

func TestResolveRows(t *testing.T) {
	testBindVars := map[string]*querypb.BindVariable{
		"int":    Int64BindVariable(10),
		"intstr": TestBindVariable([]interface{}{10, "aa"}),
	}
	intValue := MakeTrusted(Int64, []byte("10"))
	strValue := MakeTrusted(VarChar, []byte("aa"))
	tcases := []struct {
		in  []PlanValue
		out [][]Value
		err string
	}{{
		// Simple cases.
		in: []PlanValue{
			{Key: "int"},
		},
		out: [][]Value{
			{intValue},
		},
	}, {
		in: []PlanValue{
			{Value: intValue},
		},
		out: [][]Value{
			{intValue},
		},
	}, {
		in: []PlanValue{
			{ListKey: "intstr"},
		},
		out: [][]Value{
			{intValue},
			{strValue},
		},
	}, {
		in: []PlanValue{
			{Values: []PlanValue{{Value: intValue}, {Value: strValue}}},
		},
		out: [][]Value{
			{intValue},
			{strValue},
		},
	}, {
		in: []PlanValue{
			{Values: []PlanValue{{Key: "int"}, {Value: strValue}}},
		},
		out: [][]Value{
			{intValue},
			{strValue},
		},
	}, {
		in: []PlanValue{{}},
		out: [][]Value{
			{NULL},
		},
	}, {
		// Cases with varying rowcounts.
		// All types of input..
		in: []PlanValue{
			{Key: "int"},
			{Value: strValue},
			{ListKey: "intstr"},
			{Values: []PlanValue{{Value: strValue}, {Value: intValue}}},
		},
		out: [][]Value{
			{intValue, strValue, intValue, strValue},
			{intValue, strValue, strValue, intValue},
		},
	}, {
		// list, val, list.
		in: []PlanValue{
			{Value: strValue},
			{Key: "int"},
			{Values: []PlanValue{{Value: strValue}, {Value: intValue}}},
		},
		out: [][]Value{
			{strValue, intValue, strValue},
			{strValue, intValue, intValue},
		},
	}, {
		// list, list
		in: []PlanValue{
			{ListKey: "intstr"},
			{Values: []PlanValue{{Value: strValue}, {Value: intValue}}},
		},
		out: [][]Value{
			{intValue, strValue},
			{strValue, intValue},
		},
	}, {
		// Error cases
		in: []PlanValue{
			{ListKey: "intstr"},
			{Values: []PlanValue{{Value: strValue}}},
		},
		err: "mismatch in number of column values",
	}, {
		// This is a different code path for a similar validation.
		in: []PlanValue{
			{Values: []PlanValue{{Value: strValue}}},
			{ListKey: "intstr"},
		},
		err: "mismatch in number of column values",
	}, {
		in: []PlanValue{
			{Key: "absent"},
		},
		err: "missing bind var",
	}, {
		in: []PlanValue{
			{ListKey: "absent"},
		},
		err: "missing bind var",
	}, {
		in: []PlanValue{
			{Values: []PlanValue{{Key: "absent"}}},
		},
		err: "missing bind var",
	}}

	for _, tc := range tcases {
		got, err := ResolveRows(tc.in, testBindVars)
		if err != nil {
			if !strings.Contains(err.Error(), tc.err) {
				t.Errorf("ResolveRows(%v) error: %v, want '%s'", tc.in, err, tc.err)
			}
			continue
		}
		if tc.err != "" {
			t.Errorf("ResolveRows(%v) error: nil, want '%s'", tc.in, tc.err)
			continue
		}
		if !reflect.DeepEqual(got, tc.out) {
			t.Errorf("ResolveRows(%v): %v, want %v", tc.in, got, tc.out)
		}
	}
}

func TestResolveList(t *testing.T) {
	testBindVars := map[string]*querypb.BindVariable{
		"int":    Int64BindVariable(10),
		"intstr": TestBindVariable([]interface{}{10, "aa"}),
	}
	intValue := MakeTrusted(Int64, []byte("10"))
	strValue := MakeTrusted(VarChar, []byte("aa"))
	tcases := []struct {
		in  PlanValue
		out []Value
		err string
	}{{
		in:  PlanValue{ListKey: "intstr"},
		out: []Value{intValue, strValue},
	}, {
		in:  PlanValue{Values: []PlanValue{{Value: intValue}, {Value: strValue}}},
		out: []Value{intValue, strValue},
	}, {
		in:  PlanValue{Values: []PlanValue{{Key: "int"}, {Value: strValue}}},
		out: []Value{intValue, strValue},
	}, {
		in:  PlanValue{ListKey: "absent"},
		err: "missing bind var",
	}, {
		in:  PlanValue{Values: []PlanValue{{Key: "absent"}, {Value: strValue}}},
		err: "missing bind var",
	}, {
		in:  PlanValue{ListKey: "int"},
		err: "single value was supplied for TUPLE bind var",
	}, {
		in:  PlanValue{Key: "int"},
		err: "a single value was supplied where a list was expected",
	}}

	for _, tc := range tcases {
		got, err := tc.in.ResolveList(testBindVars)
		if err != nil {
			if !strings.Contains(err.Error(), tc.err) {
				t.Errorf("ResolveList(%v) error: %v, want '%s'", tc.in, err, tc.err)
			}
			continue
		}
		if tc.err != "" {
			t.Errorf("ResolveList(%v) error: nil, want '%s'", tc.in, tc.err)
			continue
		}
		if !reflect.DeepEqual(got, tc.out) {
			t.Errorf("ResolveList(%v): %v, want %v", tc.in, got, tc.out)
		}
	}
}

func TestResolveValue(t *testing.T) {
	testBindVars := map[string]*querypb.BindVariable{
		"int":    Int64BindVariable(10),
		"intstr": TestBindVariable([]interface{}{10, "aa"}),
	}
	intValue := MakeTrusted(Int64, []byte("10"))
	tcases := []struct {
		in  PlanValue
		out Value
		err string
	}{{
		in:  PlanValue{Key: "int"},
		out: intValue,
	}, {
		in:  PlanValue{Value: intValue},
		out: intValue,
	}, {
		in:  PlanValue{},
		out: NULL,
	}, {
		in:  PlanValue{Key: "absent"},
		err: "missing bind var",
	}, {
		in:  PlanValue{Key: "intstr"},
		err: "TUPLE was supplied for single value bind var",
	}, {
		in:  PlanValue{ListKey: "intstr"},
		err: "a list was supplied where a single value was expected",
	}}

	for _, tc := range tcases {
		got, err := tc.in.ResolveValue(testBindVars)
		if err != nil {
			if !strings.Contains(err.Error(), tc.err) {
				t.Errorf("ResolveValue(%v) error: %v, want '%s'", tc.in, err, tc.err)
			}
			continue
		}
		if tc.err != "" {
			t.Errorf("ResolveValue(%v) error: nil, want '%s'", tc.in, tc.err)
			continue
		}
		if !reflect.DeepEqual(got, tc.out) {
			t.Errorf("ResolveValue(%v): %v, want %v", tc.in, got, tc.out)
		}
	}
}
