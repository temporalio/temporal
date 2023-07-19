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

package quotas

type (
	// InstanceCounter returns the total number of instances there are for a given service.
	InstanceCounter interface {
		MemberCount() int
	}
	// Limits contains the per instance and per cluster limits. It exists to make it harder to mix up the two limits
	// when calling CalculateEffectiveResourceLimit.
	Limits struct {
		InstanceLimit int
		ClusterLimit  int
	}
)

// CalculateEffectiveResourceLimit returns the effective resource limit for a host given the per instance and per
// cluster limits. The "resource" here could be requests per second, total number of active requests, etc. The
// cluster-wide limit is used if and only if it is configured to a value greater than zero and the number of instances
// that membership reports is greater than zero. Otherwise, the per-instance limit is used.
func CalculateEffectiveResourceLimit(instanceCounter InstanceCounter, limits Limits) float64 {
	if clusterLimit := limits.ClusterLimit; clusterLimit > 0 && instanceCounter != nil {
		if clusterSize := instanceCounter.MemberCount(); clusterSize > 0 {
			return float64(clusterLimit) / float64(clusterSize)
		}
	}

	return float64(limits.InstanceLimit)
}
