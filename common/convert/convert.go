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

package convert

import (
	"math"
	"strconv"
)

// IntPtr makes a copy and returns the pointer to an int.
func IntPtr(v int) *int {
	return &v
}

// Int16Ptr makes a copy and returns the pointer to an int16.
func Int16Ptr(v int16) *int16 {
	return &v
}

// Int32Ptr makes a copy and returns the pointer to an int32.
func Int32Ptr(v int32) *int32 {
	return &v
}

// Int64Ptr makes a copy and returns the pointer to an int64.
func Int64Ptr(v int64) *int64 {
	return &v
}

// BoolPtr makes a copy and returns the pointer to a bool.
func BoolPtr(v bool) *bool {
	return &v
}

// StringPtr makes a copy and returns the pointer to a string.
func StringPtr(v string) *string {
	return &v
}

// Int32Ceil return the int32 ceil of a float64
func Int32Ceil(v float64) int32 {
	return int32(math.Ceil(v))
}

// Int64Ceil return the int64 ceil of a float64
func Int64Ceil(v float64) int64 {
	return int64(math.Ceil(v))
}

func IntToString(v int) string {
	return Int64ToString(int64(v))
}

func Uint64ToString(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func Int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

func Int32ToString(v int32) string {
	return Int64ToString(int64(v))
}

func Uint16ToString(v uint16) string {
	return strconv.FormatUint(uint64(v), 10)
}

func Int64SetToSlice(
	inputs map[int64]struct{},
) []int64 {
	outputs := make([]int64, len(inputs))
	i := 0
	for item := range inputs {
		outputs[i] = item
		i++
	}
	return outputs
}

func Int64SliceToSet(
	inputs []int64,
) map[int64]struct{} {
	outputs := make(map[int64]struct{}, len(inputs))
	for _, item := range inputs {
		outputs[item] = struct{}{}
	}
	return outputs
}

func StringSetToSlice(
	inputs map[string]struct{},
) []string {
	outputs := make([]string, len(inputs))
	i := 0
	for item := range inputs {
		outputs[i] = item
		i++
	}
	return outputs
}
func StringSliceToSet(
	inputs []string,
) map[string]struct{} {
	outputs := make(map[string]struct{}, len(inputs))
	for _, item := range inputs {
		outputs[item] = struct{}{}
	}
	return outputs
}
