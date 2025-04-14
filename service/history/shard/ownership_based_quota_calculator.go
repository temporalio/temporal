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
