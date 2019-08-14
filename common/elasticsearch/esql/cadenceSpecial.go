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
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) addCadenceSort(orderBySlice []string, sortFields []string) ([]string, []string, error) {
	switch len(orderBySlice) {
	case 0: // if unsorted, use default sorting
		cadenceOrderStartTime := fmt.Sprintf(`{"%v": "%v"}`, StartTime, StartTimeOrder)
		orderBySlice = append(orderBySlice, cadenceOrderStartTime)
		sortFields = append(sortFields, StartTime)
	case 1: // user should not use tieBreaker to sort
		if sortFields[0] == TieBreaker {
			err := fmt.Errorf("esql: Cadence does not allow user sort by RunID")
			return nil, nil, err
		}
	default:
		err := fmt.Errorf("esql: Cadence only allow 1 custom sort field")
		return nil, nil, err
	}

	// add tie breaker
	cadenceOrderTieBreaker := fmt.Sprintf(`{"%v": "%v"}`, TieBreaker, TieBreakerOrder)
	orderBySlice = append(orderBySlice, cadenceOrderTieBreaker)
	sortFields = append(sortFields, TieBreaker)
	return orderBySlice, sortFields, nil
}

func (e *ESql) addCadenceDomainTimeQuery(sel sqlparser.Select, domainID string, dslMap map[string]interface{}) {
	var domainIDQuery string
	if domainID != "" {
		domainIDQuery = fmt.Sprintf(`{"term": {"%v": "%v"}}`, DomainID, domainID)
	}
	if sel.Where == nil {
		if domainID != "" {
			dslMap["query"] = domainIDQuery
		}
	} else {
		if domainID != "" {
			domainIDQuery = domainIDQuery + ","
		}
		if strings.Contains(fmt.Sprintf("%v", dslMap["query"]), ExecutionTime) {
			executionTimeBound := fmt.Sprintf(`{"range": {"%v": {"gte": "0"}}}`, ExecutionTime)
			dslMap["query"] = fmt.Sprintf(`{"bool": {"filter": [%v %v, %v]}}`, domainIDQuery, executionTimeBound, dslMap["query"])
		} else {
			dslMap["query"] = fmt.Sprintf(`{"bool": {"filter": [%v %v]}}`, domainIDQuery, dslMap["query"])
		}
	}
}
