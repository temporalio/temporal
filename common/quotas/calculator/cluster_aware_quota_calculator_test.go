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
