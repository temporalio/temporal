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

package host

import (
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/searchattribute"
)

type (
	SearchAttributeTestMapper struct{}
)

func NewSearchAttributeTestMapper() *SearchAttributeTestMapper {
	return &SearchAttributeTestMapper{}
}

func (t *SearchAttributeTestMapper) GetAlias(fieldName string, namespace string) (string, error) {
	if _, err := searchattribute.TestNameTypeMap.GetType(fieldName); err == nil {
		return "AliasFor" + fieldName, nil
	}
	return "", serviceerror.NewInvalidArgument(fmt.Sprintf("fieldname '%s' has no search-attribute defined for '%s' namespace", fieldName, namespace))
}

func (t *SearchAttributeTestMapper) GetFieldName(alias string, namespace string) (string, error) {
	if strings.HasPrefix(alias, "AliasFor") {
		fieldName := strings.TrimPrefix(alias, "AliasFor")
		if _, err := searchattribute.TestNameTypeMap.GetType(fieldName); err == nil {
			return fieldName, nil
		}
	}
	return "", serviceerror.NewInvalidArgument(fmt.Sprintf("search-attribute '%s' not found for '%s' namespace", alias, namespace))
}
