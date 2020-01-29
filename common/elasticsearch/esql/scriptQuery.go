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

func (e *ESql) convertToScript(expr sqlparser.Expr) (script string, err error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		script, err = e.convertColName(expr)
		script = fmt.Sprintf(`doc['%v'].value`, script)
	case *sqlparser.SQLVal:
		script, err = e.convertValExpr(expr, true)
	case *sqlparser.BinaryExpr:
		script, err = e.convertBinaryExpr(expr)
	case *sqlparser.ParenExpr:
		script, err = e.convertToScript(expr.Expr)
		script = fmt.Sprintf(`(%v)`, script)
	default:
		err = fmt.Errorf("esql: invalid expression type for scripting")
	}
	if err != nil {
		return "", err
	}
	return script, nil
}

func (e *ESql) convertBinaryExpr(expr sqlparser.Expr) (string, error) {
	var script, lhsStr, rhsStr string
	var err error
	binExpr, ok := expr.(*sqlparser.BinaryExpr)
	if !ok {
		err = fmt.Errorf("esql: invalid binary expression")
		return "", err
	}
	lhsExpr, rhsExpr := binExpr.Left, binExpr.Right
	op, ok := opBinaryExpr[binExpr.Operator]
	if !ok {
		err = fmt.Errorf("esql: not supported binary expression operator")
		return "", err
	}

	lhsStr, err = e.convertToScript(lhsExpr)
	if err != nil {
		return "", err
	}
	rhsStr, err = e.convertToScript(rhsExpr)
	if err != nil {
		return "", err
	}

	script = fmt.Sprintf(`%v %v %v`, lhsStr, op, rhsStr)
	return script, nil
}
