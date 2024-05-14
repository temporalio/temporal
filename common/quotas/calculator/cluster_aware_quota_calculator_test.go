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

package calculator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas/quotastest"
)

type quotaCalculatorTestCase struct {
	name          string
	memberCounter MemberCounter
	instanceLimit int
	clusterLimit  int
	expected      float64
}

var quotaCalculatorTestCases = []quotaCalculatorTestCase{
	{
		name:          "both limits set",
		memberCounter: quotastest.NewFakeMemberCounter(4),
		instanceLimit: 10,
		clusterLimit:  20,
		expected:      5.0,
	},
	{
		name:          "no per cluster limit",
		memberCounter: quotastest.NewFakeMemberCounter(4),
		instanceLimit: 10,
		clusterLimit:  0,
		expected:      10.0,
	},
	{
		name:          "no hosts",
		memberCounter: quotastest.NewFakeMemberCounter(0),
		instanceLimit: 10,
		clusterLimit:  20,
		expected:      10.0,
	},
	{
		name:          "nil member counter",
		memberCounter: nil,
		instanceLimit: 10,
		clusterLimit:  20,
		expected:      10.0,
	},
}

func TestClusterAwareQuotaCalculator_GetQuota(t *testing.T) {
	t.Parallel()

	for _, tc := range quotaCalculatorTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, ClusterAwareQuotaCalculator{
				MemberCounter:    tc.memberCounter,
				PerInstanceQuota: dynamicconfig.GetIntPropertyFn(tc.instanceLimit),
				GlobalQuota:      dynamicconfig.GetIntPropertyFn(tc.clusterLimit),
			}.GetQuota())

		})
	}
}

type perNamespaceQuota struct {
	t     *testing.T
	quota int
}

func (l perNamespaceQuota) getQuota(ns string) int {
	if ns != "test-namespace" {
		l.t.Errorf("unexpected namespace: %s", ns)
	}
	return l.quota
}

func TestClusterAwareNamespaceSpecificQuotaCalculator_GetQuota(t *testing.T) {
	t.Parallel()

	for _, tc := range quotaCalculatorTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			instanceLimit := perNamespaceQuota{t: t, quota: tc.instanceLimit}

			clusterLimit := perNamespaceQuota{t: t, quota: tc.clusterLimit}

			assert.Equal(t, tc.expected, ClusterAwareNamespaceQuotaCalculator{
				MemberCounter:    tc.memberCounter,
				PerInstanceQuota: instanceLimit.getQuota,
				GlobalQuota:      clusterLimit.getQuota,
			}.GetQuota("test-namespace"))
		})
	}
}
