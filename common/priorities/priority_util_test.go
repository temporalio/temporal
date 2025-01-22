// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2025 Uber Technologies, Inc.
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

package priorities

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestMerge(t *testing.T) {
	defaultPriority := &commonpb.Priority{}

	testcases := []struct {
		name     string
		base     *commonpb.Priority
		override *commonpb.Priority
		expected *commonpb.Priority
	}{
		{
			name:     "all nil",
			base:     nil,
			override: nil,
			expected: nil,
		},
		{
			name:     "base is nil",
			base:     defaultPriority,
			override: nil,
			expected: defaultPriority,
		},
		{
			name:     "override is nil",
			base:     nil,
			override: defaultPriority,
			expected: defaultPriority,
		},
		{
			name:     "priority key is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{PriorityKey: 5},
			expected: &commonpb.Priority{PriorityKey: 5},
		},
		{
			name:     "priority key is not overriden by default value",
			base:     &commonpb.Priority{PriorityKey: 1},
			override: defaultPriority,
			expected: &commonpb.Priority{PriorityKey: 1},
		},
		{
			name:     "fairness key is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{FairnessKey: "override"},
			expected: &commonpb.Priority{FairnessKey: "override"},
		},
		{
			name:     "fairness key is not overriden by default value",
			base:     &commonpb.Priority{FairnessKey: "base"},
			override: defaultPriority,
			expected: &commonpb.Priority{FairnessKey: "base"},
		},
		{
			name:     "fairness weight is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{FairnessWeight: 1},
			expected: &commonpb.Priority{FairnessWeight: 1},
		},
		{
			name:     "fairness weight is not overriden by default value",
			base:     &commonpb.Priority{FairnessWeight: 0.001},
			override: defaultPriority,
			expected: &commonpb.Priority{FairnessWeight: 0.001},
		},
		{
			name:     "fairness ratelimit is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{FairnessRateLimit: 1},
			expected: &commonpb.Priority{FairnessRateLimit: 1},
		},
		{
			name:     "fairness ratelimit is not overriden by default value",
			base:     &commonpb.Priority{FairnessRateLimit: 0.001},
			override: defaultPriority,
			expected: &commonpb.Priority{FairnessRateLimit: 0.001},
		},
		{
			name:     "ordering key is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{OrderingKey: 42},
			expected: &commonpb.Priority{OrderingKey: 42},
		},
		{
			name:     "ordering key is not overriden by default value",
			base:     &commonpb.Priority{OrderingKey: 1},
			override: defaultPriority,
			expected: &commonpb.Priority{OrderingKey: 1},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := Merge(tc.base, tc.override)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}
