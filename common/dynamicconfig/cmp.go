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

package dynamicconfig

import (
	"math"
	"time"
)

func float64CompareEquals(a, b interface{}) bool {
	aVal := a.(float64)
	bVal := b.(float64)
	return (aVal == bVal) || math.Nextafter(aVal, bVal) == aVal
}

func intCompareEquals(a, b interface{}) bool {
	aVal := a.(int)
	bVal := b.(int)
	return aVal == bVal
}

func boolCompareEquals(a, b interface{}) bool {
	aVal := a.(bool)
	bVal := b.(bool)
	return aVal == bVal
}

func stringCompareEquals(a, b interface{}) bool {
	aVal := a.(string)
	bVal := b.(string)
	return aVal == bVal
}

func durationCompareEquals(a, b interface{}) bool {
	aVal := a.(time.Duration)
	bVal := b.(time.Duration)
	return aVal.Nanoseconds() == bVal.Nanoseconds()
}
