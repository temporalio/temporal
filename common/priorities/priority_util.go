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

package priorities

import (
	"cmp"

	commonpb "go.temporal.io/api/common/v1"
)

func Merge(
	base *commonpb.Priority,
	override *commonpb.Priority,
) *commonpb.Priority {
	if base == nil || override == nil {
		return cmp.Or(base, override)
	}

	priorityKey := base.PriorityKey
	if override.PriorityKey != 0 {
		priorityKey = override.PriorityKey
	}

	fairnessKey := base.FairnessKey
	if override.FairnessKey != "" {
		fairnessKey = override.FairnessKey
	}

	fairnessWeight := base.FairnessWeight
	if override.FairnessWeight != 0 {
		fairnessWeight = override.FairnessWeight
	}

	fairnessRateLimit := base.FairnessRateLimit
	if override.FairnessRateLimit != 0 {
		fairnessRateLimit = override.FairnessRateLimit
	}

	orderingKey := base.OrderingKey
	if override.OrderingKey != 0 {
		orderingKey = override.OrderingKey
	}

	return &commonpb.Priority{
		PriorityKey:       priorityKey,
		FairnessKey:       fairnessKey,
		FairnessWeight:    fairnessWeight,
		FairnessRateLimit: fairnessRateLimit,
		OrderingKey:       orderingKey,
	}
}
