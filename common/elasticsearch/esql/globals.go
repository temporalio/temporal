// Copyright (c) 2017 Uber Technologies, Inc.
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

package esql

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// used for invert operator when NOT is specified
var oppositeOperator = map[string]string{
	"=":                     "!=",
	"!=":                    "=",
	"<":                     ">=",
	"<=":                    ">",
	">":                     "<=",
	">=":                    "<",
	"<>":                    "=",
	"in":                    "not in",
	"like":                  "not like",
	"regexp":                "not regexp",
	"not in":                "in",
	"not like":              "like",
	"not regexp":            "regexp",
	sqlparser.IsNullStr:     sqlparser.IsNotNullStr,
	sqlparser.IsNotNullStr:  sqlparser.IsNullStr,
	sqlparser.BetweenStr:    sqlparser.NotBetweenStr,
	sqlparser.NotBetweenStr: sqlparser.BetweenStr,
}

// used for convert SQL operator to painless operator in HAVING expression
var op2PainlessOp = map[string]string{
	"=":  "==",
	"!=": "!==",
	"<":  "<",
	"<=": "<=",
	">":  ">",
	">=": ">=",
	"<>": "!==",
}

var opBinaryExpr = map[string]string{
	"|":  "|",
	"&":  "&",
	"^":  "^",
	"+":  "+",
	"-":  "-",
	"*":  "*",
	"/":  "/",
	"%":  "%",
	">>": ">>",
	"<<": "<<",
}

// default sizes and identifiers used in cadence visibility
const (
	DefaultPageSize      = 1000
	DefaultBucketNumber  = 1000
	ESDefaultMaxPageSize = 10000
	TieBreaker           = "RunID"
	RunID                = "RunID"
	StartTime            = "StartTime"
	DomainID             = "DomainID"
	WorkflowID           = "WorkflowID"
	ExecutionTime        = "ExecutionTime"
	TieBreakerOrder      = "desc"
	StartTimeOrder       = "desc"
)

// DEBUG usage
//nolint
func print(v interface{}) {
	fmt.Println("==============")
	fmt.Println(v)
	fmt.Println("==============")
}
