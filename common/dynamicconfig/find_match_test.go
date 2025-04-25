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
	"testing"

	"github.com/stretchr/testify/assert"
)

// These two tests are in a separate file in the 'dynamicconfig' package to access the private
// findMatch function. Most other tests should be in 'dynamicconfig_test' to test things as a
// client.
func TestFindMatch(t *testing.T) {
	testCases := []struct {
		v       []ConstrainedValue
		filters []Constraints
		matched bool
	}{
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			matched: false,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"}},
			},
			filters: []Constraints{
				{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"},
			},
			matched: true,
		},
		{
			v: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{TaskQueueName: "sample-task-queue"},
			},
			matched: false,
		},
	}

	for _, tc := range testCases {
		_, err := findMatch(tc.v, tc.filters)
		assert.Equal(t, tc.matched, err == nil)
	}
}

func TestFindMatchWithTyped(t *testing.T) {
	testCases := []struct {
		val      []ConstrainedValue
		tv       []TypedConstrainedValue[struct{}]
		filters  []Constraints
		valOrder int
		defOrder int
	}{
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			valOrder: 0,
			defOrder: 0,
		},
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{Namespace: "some random namespace"},
			},
			valOrder: 0,
			defOrder: 0,
		},
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"}},
			},
			filters: []Constraints{
				{Namespace: "samples-namespace", TaskQueueName: "sample-task-queue"},
			},
			valOrder: 0,
			defOrder: 1,
		},
		{
			val: nil,
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "samples-namespace"}},
			},
			filters: []Constraints{
				{TaskQueueName: "sample-task-queue"},
			},
			valOrder: 0,
			defOrder: 0,
		},
		{
			val: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "ns"}},
			},
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "ns", TaskQueueName: "othertq"}},
				{},
			},
			filters: []Constraints{
				{Namespace: "ns", TaskQueueName: "tq"},
				{Namespace: "ns"},
				{},
			},
			valOrder: 4,
			defOrder: 9,
		},
		{
			val: []ConstrainedValue{
				{Constraints: Constraints{Namespace: "ns"}},
			},
			tv: []TypedConstrainedValue[struct{}]{
				{Constraints: Constraints{Namespace: "ns", TaskQueueName: "tq"}},
				{},
			},
			filters: []Constraints{
				{Namespace: "ns", TaskQueueName: "tq"},
				{Namespace: "ns"},
				{},
			},
			valOrder: 4,
			defOrder: 2,
		},
	}

	for _, tc := range testCases {
		_, _, valOrder, defOrder := findMatchWithConstrainedDefaults(tc.val, tc.tv, tc.filters)
		assert.Equal(t, tc.valOrder, valOrder)
		assert.Equal(t, tc.defOrder, defOrder)
	}
}
