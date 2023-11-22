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

package enums

import (
	"fmt"
)

var (
	PredicateType_shorthandValue = map[string]int32{
		"Unspecified": 0,
		"Universal":   1,
		"Empty":       2,
		"And":         3,
		"Or":          4,
		"Not":         5,
		"NamespaceId": 6,
		"TaskType":    7,
	}
)

// PredicateTypeFromString parses a PredicateType value from  either the protojson
// canonical SCREAMING_CASE enum or the traditional temporal PascalCase enum to PredicateType
func PredicateTypeFromString(s string) (PredicateType, error) {
	if v, ok := PredicateType_value[s]; ok {
		return PredicateType(v), nil
	} else if v, ok := PredicateType_shorthandValue[s]; ok {
		return PredicateType(v), nil
	}
	return PredicateType(0), fmt.Errorf("%s is not a valid PredicateType", s)
}
