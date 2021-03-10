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

package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) addTemporalSort(orderBySlice []string, sortFields []string) ([]string, []string, error) {
	switch len(orderBySlice) {
	case 0: // if unsorted, use default sorting
		temporalOrderStartTime := fmt.Sprintf(`{"%v": "%v"}`, StartTime, StartTimeOrder)
		orderBySlice = append(orderBySlice, temporalOrderStartTime)
		sortFields = append(sortFields, StartTime)
	case 1: // user should not use tieBreaker to sort
		if sortFields[0] == TieBreaker {
			err := fmt.Errorf("esql: Temporal does not allow user sort by RunId")
			return nil, nil, err
		}
	default:
		err := fmt.Errorf("esql: Temporal only allow 1 custom sort field")
		return nil, nil, err
	}

	// add tie breaker
	temporalOrderTieBreaker := fmt.Sprintf(`{"%v": "%v"}`, TieBreaker, TieBreakerOrder)
	orderBySlice = append(orderBySlice, temporalOrderTieBreaker)
	sortFields = append(sortFields, TieBreaker)
	return orderBySlice, sortFields, nil
}

func (e *ESql) addTemporalNamespaceTimeQuery(sel sqlparser.Select, namespaceID string, dslMap map[string]interface{}) {
	var namespaceIDQuery string
	if namespaceID != "" {
		namespaceIDQuery = fmt.Sprintf(`{"term": {"%v": "%v"}}`, NamespaceID, namespaceID)
	}
	if sel.Where == nil {
		if namespaceID != "" {
			dslMap["query"] = namespaceIDQuery
		}
	} else {
		if namespaceID != "" {
			namespaceIDQuery = namespaceIDQuery + ","
		}
		if strings.Contains(fmt.Sprintf("%v", dslMap["query"]), ExecutionTime) {
			executionTimeBound := fmt.Sprintf(`{"range": {"%v": {"gte": "0"}}}`, ExecutionTime)
			dslMap["query"] = fmt.Sprintf(`{"bool": {"filter": [%v %v, %v]}}`, namespaceIDQuery, executionTimeBound, dslMap["query"])
		} else {
			dslMap["query"] = fmt.Sprintf(`{"bool": {"filter": [%v %v]}}`, namespaceIDQuery, dslMap["query"])
		}
	}
}
