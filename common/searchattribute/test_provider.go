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

package searchattribute

import (
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	TestProvider struct{}
)

var (
	TestNameTypeMap = NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomIntField":      enumspb.INDEXED_VALUE_TYPE_INT,
			"CustomTextField":     enumspb.INDEXED_VALUE_TYPE_TEXT,
			"CustomKeywordField":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomDatetimeField": enumspb.INDEXED_VALUE_TYPE_DATETIME,
			"CustomDoubleField":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"CustomBoolField":     enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	}
)

func NewTestProvider() *TestProvider {
	return &TestProvider{}
}

func (s *TestProvider) GetSearchAttributes(_ string, _ bool) (NameTypeMap, error) {
	return TestNameTypeMap, nil
}
