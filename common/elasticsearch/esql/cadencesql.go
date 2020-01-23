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
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// SetCadence ... specify whether do special handling for cadence visibility
// should not be called if there is potential race condition
// should not be called by non-cadence user
func (e *ESql) SetCadence(cadenceArg bool) {
	e.cadence = cadenceArg
}

// ConvertPrettyCadence ...
// convert sql to es dsl, for cadence usage
func (e *ESql) ConvertPrettyCadence(sql string, domainID string, pagination ...interface{}) (dsl string, sortFields []string, err error) {
	dsl, sortFields, err = e.ConvertCadence(sql, domainID, pagination...)
	if err != nil {
		return "", nil, err
	}

	var prettifiedDSLBytes bytes.Buffer
	err = json.Indent(&prettifiedDSLBytes, []byte(dsl), "", "  ")
	if err != nil {
		return "", nil, err
	}
	return prettifiedDSLBytes.String(), sortFields, err
}

// ConvertCadence ...
// convert sql to es dsl, for cadence usage
func (e *ESql) ConvertCadence(sql string, domainID string, pagination ...interface{}) (dsl string, sortFields []string, err error) {
	if !e.cadence {
		err = fmt.Errorf(`esql: cadence option not turned on`)
		return "", nil, err
	}
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", nil, err
	}

	//sql valid, start to handle
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		dsl, sortFields, err = e.convertSelect(*(stmt), domainID, pagination...)
	default:
		err = fmt.Errorf(`esql: Queries other than select not supported`)
	}

	if err != nil {
		return "", nil, err
	}
	return dsl, sortFields, nil
}
