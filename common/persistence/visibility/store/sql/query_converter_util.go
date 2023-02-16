// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package sql

import (
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

type (
	// unsafeSQLString don't escape the string value; unlike sqlparser.SQLVal.
	// This is used for building string known to be safe.
	unsafeSQLString struct {
		sqlparser.Expr
		Val string
	}

	colName struct {
		sqlparser.Expr
		Name string
	}
)

const (
	coalesceFuncName = "coalesce"
)

var _ sqlparser.Expr = (*unsafeSQLString)(nil)
var _ sqlparser.Expr = (*colName)(nil)

var (
	maxDatetimeValue = getMaxDatetimeValue()
)

func (node *unsafeSQLString) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("'%s'", node.Val)
}

func (node *colName) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("%s", node.Name)
}

func newUnsafeSQLString(val string) *unsafeSQLString {
	return &unsafeSQLString{Val: val}
}

func newColName(name string) *colName {
	return &colName{Name: name}
}

func newFuncExpr(name string, exprs ...sqlparser.Expr) *sqlparser.FuncExpr {
	args := make([]sqlparser.SelectExpr, len(exprs))
	for i := range exprs {
		args[i] = &sqlparser.AliasedExpr{Expr: exprs[i]}
	}
	return &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent(name),
		Exprs: args,
	}
}

func addPrefix(prefix string, fields []string) []string {
	out := make([]string, len(fields))
	for i, field := range fields {
		out[i] = prefix + field
	}
	return out
}

func getMaxDatetimeValue() time.Time {
	t, _ := time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")
	return t
}

// Simple tokenizer by spaces. It's a temporary solution as it doesn't cover tokenizer used by
// PostgreSQL or SQLite.
func tokenizeTextQueryString(s string) []string {
	tokens := strings.Split(s, " ")
	nonEmptyTokens := make([]string, 0, len(tokens))
	for _, token := range tokens {
		if token != "" {
			nonEmptyTokens = append(nonEmptyTokens, token)
		}
	}
	return nonEmptyTokens
}

func getUnsafeStringTupleValues(valTuple sqlparser.ValTuple) ([]string, error) {
	values := make([]string, len(valTuple))
	for i, val := range valTuple {
		switch v := val.(type) {
		case *unsafeSQLString:
			values[i] = v.Val
		default:
			return nil, query.NewConverterError(
				"%s: unexpected value type in tuple (expected string, got %v)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(v),
			)
		}
	}
	return values, nil
}
