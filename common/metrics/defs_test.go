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

package metrics

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestScopeDefsMapped(t *testing.T) {
	for i := PersistenceCreateShardScope; i < NumCommonScopes; i++ {
		key, ok := ScopeDefs[Common][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := FrontendStartWorkflowExecutionScope; i < NumFrontendScopes; i++ {
		key, ok := ScopeDefs[Frontend][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := HistoryStartWorkflowExecutionScope; i < NumHistoryScopes; i++ {
		key, ok := ScopeDefs[History][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := MatchingPollForDecisionTaskScope; i < NumMatchingScopes; i++ {
		key, ok := ScopeDefs[Matching][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := ReplicatorScope; i < NumWorkerScopes; i++ {
		key, ok := ScopeDefs[Worker][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
}

func TestMetricDefsMapped(t *testing.T) {
	for i := CadenceRequests; i < NumCommonMetrics; i++ {
		key, ok := MetricDefs[Common][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := TaskRequests; i < NumHistoryMetrics; i++ {
		key, ok := MetricDefs[History][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := PollSuccessCounter; i < NumMatchingMetrics; i++ {
		key, ok := MetricDefs[Matching][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
	for i := ReplicatorMessages; i < NumWorkerMetrics; i++ {
		key, ok := MetricDefs[Worker][i]
		require.True(t, ok)
		require.NotEmpty(t, key)
	}
}
