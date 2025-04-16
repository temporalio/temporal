// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package chasm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChasmFieldTypePrefix(t *testing.T) {
	f := Field[any]{}
	fT := reflect.TypeOf(f)
	assert.True(t, strings.HasPrefix(fT.String(), chasmFieldTypePrefix))
}

func TestChasmCollectionTypePrefix(t *testing.T) {
	c := Collection[any]{}
	cT := reflect.TypeOf(c)
	assert.True(t, strings.HasPrefix(cT.String(), chasmCollectionTypePrefix))
}

func TestInternalFieldName(t *testing.T) {
	f := Field[any]{}
	fT := reflect.TypeOf(f)

	_, ok := fT.FieldByName(internalFieldName)
	assert.True(t, ok, "expected field %s not found", internalFieldName)
}

func TestGenericTypePrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "Field type",
			input:    Field[string]{},
			expected: chasmFieldTypePrefix,
		},
		{
			name:     "Collection type",
			input:    Collection[int]{},
			expected: chasmCollectionTypePrefix,
		},
		{
			name:     "Non-generic type",
			input:    0,
			expected: "",
		},
		{
			name:     "Map type",
			input:    map[string]int{},
			expected: "map[",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ := reflect.TypeOf(tt.input)
			result := genericTypePrefix(typ)
			assert.Equal(t, tt.expected, result)
		})
	}
}
