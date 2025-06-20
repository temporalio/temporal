package shard

import (
	"go.temporal.io/server/common/quotas/calculator"
)

type (
	OwnershipAwareQuotaCalculator struct {
		calculator.ClusterAwareQuotaCalculator

		scaler OwnershipBasedQuotaScaler
	}

	OwnershipAwareNamespaceQuotaCalculator struct {
		calculator.ClusterAwareNamespaceQuotaCalculator

		scaler OwnershipBasedQuotaScaler
	}
)

func NewOwnershipAwareQuotaCalculator(
	scaler OwnershipBasedQuotaScaler,
	memberCounter calculator.MemberCounter,
	perInstanceQuota func() int,
	globalQuota func() int,
) *OwnershipAwareQuotaCalculator {
	return &OwnershipAwareQuotaCalculator{
		ClusterAwareQuotaCalculator: calculator.ClusterAwareQuotaCalculator{
			MemberCounter:    memberCounter,
			PerInstanceQuota: perInstanceQuota,
			GlobalQuota:      globalQuota,
		},
		scaler: scaler,
	}
}

func (c *OwnershipAwareQuotaCalculator) GetQuota() float64 {
	if quota, ok := getOwnershipScaledQuota(c.scaler, c.GlobalQuota()); ok {
		return quota
	}
	return c.ClusterAwareQuotaCalculator.GetQuota()
}

func NewOwnershipAwareNamespaceQuotaCalculator(
	scaler OwnershipBasedQuotaScaler,
	memberCounter calculator.MemberCounter,
	perInstanceQuota func(namespace string) int,
	globalQuota func(namespace string) int,
) *OwnershipAwareNamespaceQuotaCalculator {
	return &OwnershipAwareNamespaceQuotaCalculator{
		ClusterAwareNamespaceQuotaCalculator: calculator.ClusterAwareNamespaceQuotaCalculator{
			MemberCounter:    memberCounter,
			PerInstanceQuota: perInstanceQuota,
			GlobalQuota:      globalQuota,
		},
		scaler: scaler,
	}
}

func (c *OwnershipAwareNamespaceQuotaCalculator) GetQuota(namespace string) float64 {
	if quota, ok := getOwnershipScaledQuota(c.scaler, c.GlobalQuota(namespace)); ok {
		return quota
	}
	return c.ClusterAwareNamespaceQuotaCalculator.GetQuota(namespace)
}

func getOwnershipScaledQuota(
	scaler OwnershipBasedQuotaScaler,
	globalLimit int,
) (float64, bool) {
	if globalLimit > 0 && scaler != nil {
		if scaleFactor, ok := scaler.ScaleFactor(); ok {
			return scaleFactor * float64(globalLimit), true
		}
	}
	return 0, false
}
