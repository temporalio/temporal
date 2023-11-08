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
	"testing"
)

func TestSplitComments(t *testing.T) {
	var testCases = []struct {
		input, outSQL, outLeadingComments, outTrailingComments string
	}{{
		input:               "/",
		outSQL:              "/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "*/",
		outSQL:              "*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*/",
		outSQL:              "/*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "a*/",
		outSQL:              "a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "*a*/",
		outSQL:              "*a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "**a*/",
		outSQL:              "**a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*b**a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*b**a*/",
	}, {
		input:               "/*a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*a*/",
	}, {
		input:               "/**/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/**/",
	}, {
		input:               "/*b*/ /*a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*b*/ /*a*/",
	}, {
		input:               "/* before */ foo /* bar */",
		outSQL:              "foo",
		outLeadingComments:  "/* before */ ",
		outTrailingComments: " /* bar */",
	}, {
		input:               "/* before1 */ /* before2 */ foo /* after1 */ /* after2 */",
		outSQL:              "foo",
		outLeadingComments:  "/* before1 */ /* before2 */ ",
		outTrailingComments: " /* after1 */ /* after2 */",
	}, {
		input:               "/** before */ foo /** bar */",
		outSQL:              "foo",
		outLeadingComments:  "/** before */ ",
		outTrailingComments: " /** bar */",
	}, {
		input:               "/*** before */ foo /*** bar */",
		outSQL:              "foo",
		outLeadingComments:  "/*** before */ ",
		outTrailingComments: " /*** bar */",
	}, {
		input:               "/** before **/ foo /** bar **/",
		outSQL:              "foo",
		outLeadingComments:  "/** before **/ ",
		outTrailingComments: " /** bar **/",
	}, {
		input:               "/*** before ***/ foo /*** bar ***/",
		outSQL:              "foo",
		outLeadingComments:  "/*** before ***/ ",
		outTrailingComments: " /*** bar ***/",
	}, {
		input:               " /*** before ***/ foo /*** bar ***/ ",
		outSQL:              "foo",
		outLeadingComments:  "/*** before ***/ ",
		outTrailingComments: " /*** bar ***/",
	}, {
		input:               "*** bar ***/",
		outSQL:              "*** bar ***/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               " foo ",
		outSQL:              "foo",
		outLeadingComments:  "",
		outTrailingComments: "",
	}}
	for _, testCase := range testCases {
		gotSQL, gotComments := SplitMarginComments(testCase.input)
		gotLeadingComments, gotTrailingComments := gotComments.Leading, gotComments.Trailing

		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
		if gotLeadingComments != testCase.outLeadingComments {
			t.Errorf("test input: '%s', got LeadingComments\n%+v, want\n%+v", testCase.input, gotLeadingComments, testCase.outLeadingComments)
		}
		if gotTrailingComments != testCase.outTrailingComments {
			t.Errorf("test input: '%s', got TrailingComments\n%+v, want\n%+v", testCase.input, gotTrailingComments, testCase.outTrailingComments)
		}
	}
}

func TestStripLeadingComments(t *testing.T) {
	var testCases = []struct {
		input, outSQL string
	}{{
		input:  "/",
		outSQL: "/",
	}, {
		input:  "*/",
		outSQL: "*/",
	}, {
		input:  "/*/",
		outSQL: "/*/",
	}, {
		input:  "/*a",
		outSQL: "/*a",
	}, {
		input:  "/*a*",
		outSQL: "/*a*",
	}, {
		input:  "/*a**",
		outSQL: "/*a**",
	}, {
		input:  "/*b**a*/",
		outSQL: "",
	}, {
		input:  "/*a*/",
		outSQL: "",
	}, {
		input:  "/**/",
		outSQL: "",
	}, {
		input:  "/*!*/",
		outSQL: "/*!*/",
	}, {
		input:  "/*!a*/",
		outSQL: "/*!a*/",
	}, {
		input:  "/*b*/ /*a*/",
		outSQL: "",
	}, {
		input: `/*b*/ --foo
bar`,
		outSQL: "bar",
	}, {
		input:  "foo /* bar */",
		outSQL: "foo /* bar */",
	}, {
		input:  "/* foo */ bar",
		outSQL: "bar",
	}, {
		input:  "-- /* foo */ bar",
		outSQL: "-- /* foo */ bar",
	}, {
		input:  "foo -- bar */",
		outSQL: "foo -- bar */",
	}, {
		input: `/*
foo */ bar`,
		outSQL: "bar",
	}, {
		input: `-- foo bar
a`,
		outSQL: "a",
	}, {
		input:  `-- foo bar`,
		outSQL: "-- foo bar",
	}}
	for _, testCase := range testCases {
		gotSQL := StripLeadingComments(testCase.input)

		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
	}
}

func TestExtractMysqlComment(t *testing.T) {
	var testCases = []struct {
		input, outSQL, outVersion string
	}{{
		input:      "/*!50708SET max_execution_time=5000 */",
		outSQL:     "SET max_execution_time=5000",
		outVersion: "50708",
	}, {
		input:      "/*!50708 SET max_execution_time=5000*/",
		outSQL:     "SET max_execution_time=5000",
		outVersion: "50708",
	}, {
		input:      "/*!50708* from*/",
		outSQL:     "* from",
		outVersion: "50708",
	}, {
		input:      "/*! SET max_execution_time=5000*/",
		outSQL:     "SET max_execution_time=5000",
		outVersion: "",
	}}
	for _, testCase := range testCases {
		gotVersion, gotSQL := ExtractMysqlComment(testCase.input)

		if gotVersion != testCase.outVersion {
			t.Errorf("test input: '%s', got version\n%+v, want\n%+v", testCase.input, gotVersion, testCase.outVersion)
		}
		if gotSQL != testCase.outSQL {
			t.Errorf("test input: '%s', got SQL\n%+v, want\n%+v", testCase.input, gotSQL, testCase.outSQL)
		}
	}
}

func TestExtractCommentDirectives(t *testing.T) {
	var testCases = []struct {
		input string
		vals  CommentDirectives
	}{{
		input: "",
		vals:  nil,
	}, {
		input: "/* not a vt comment */",
		vals:  nil,
	}, {
		input: "/*vt+ */",
		vals:  CommentDirectives{},
	}, {
		input: "/*vt+ SINGLE_OPTION */",
		vals: CommentDirectives{
			"SINGLE_OPTION": true,
		},
	}, {
		input: "/*vt+ ONE_OPT TWO_OPT */",
		vals: CommentDirectives{
			"ONE_OPT": true,
			"TWO_OPT": true,
		},
	}, {
		input: "/*vt+ ONE_OPT */ /* other comment */ /*vt+ TWO_OPT */",
		vals: CommentDirectives{
			"ONE_OPT": true,
			"TWO_OPT": true,
		},
	}, {
		input: "/*vt+ ONE_OPT=abc TWO_OPT=def */",
		vals: CommentDirectives{
			"ONE_OPT": "abc",
			"TWO_OPT": "def",
		},
	}, {
		input: "/*vt+ ONE_OPT=true TWO_OPT=false */",
		vals: CommentDirectives{
			"ONE_OPT": true,
			"TWO_OPT": false,
		},
	}, {
		input: "/*vt+ ONE_OPT=true TWO_OPT=\"false\" */",
		vals: CommentDirectives{
			"ONE_OPT": true,
			"TWO_OPT": "\"false\"",
		},
	}, {
		input: "/*vt+ RANGE_OPT=[a:b] ANOTHER ANOTHER_WITH_VALEQ=val= AND_ONE_WITH_EQ== */",
		vals: CommentDirectives{
			"RANGE_OPT":          "[a:b]",
			"ANOTHER":            true,
			"ANOTHER_WITH_VALEQ": "val=",
			"AND_ONE_WITH_EQ":    "=",
		},
	}}

	for _, testCase := range testCases {
		sql := "select " + testCase.input + " 1 from dual"
		stmt, _ := Parse(sql)
		comments := stmt.(*Select).Comments
		vals := ExtractCommentDirectives(comments)

		if !reflect.DeepEqual(vals, testCase.vals) {
			t.Errorf("test input: '%v', got vals:\n%+v, want\n%+v", testCase.input, vals, testCase.vals)
		}
	}

	d := CommentDirectives{
		"ONE_OPT": true,
		"TWO_OPT": false,
		"three":   1,
		"four":    2,
		"five":    0,
		"six":     "true",
	}

	if !d.IsSet("ONE_OPT") {
		t.Errorf("d.IsSet(ONE_OPT) should be true")
	}

	if d.IsSet("TWO_OPT") {
		t.Errorf("d.IsSet(TWO_OPT) should be false")
	}

	if !d.IsSet("three") {
		t.Errorf("d.IsSet(three) should be true")
	}

	if d.IsSet("four") {
		t.Errorf("d.IsSet(four) should be false")
	}

	if d.IsSet("five") {
		t.Errorf("d.IsSet(five) should be false")
	}

	if d.IsSet("six") {
		t.Errorf("d.IsSet(six) should be false")
	}
}

func TestSkipQueryPlanCacheDirective(t *testing.T) {
	stmt, _ := Parse("insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}

	stmt, _ = Parse("insert into user(id) values (1), (2)")
	if SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be false")
	}

	stmt, _ = Parse("update /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ users set name=1")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}

	stmt, _ = Parse("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from users")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}

	stmt, _ = Parse("delete /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ from users")
	if !SkipQueryPlanCacheDirective(stmt) {
		t.Errorf("d.SkipQueryPlanCacheDirective(stmt) should be true")
	}
}
