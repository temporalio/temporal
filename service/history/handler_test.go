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
package history

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestDescribeHistoryHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	controller := shard.NewMockController(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	h := Handler{
		config: &configs.Config{
			NumberOfShards: 10,
		},
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewNoopLogger(),
		controller:        controller,
		namespaceRegistry: namespaceRegistry,
		hostInfoProvider:  hostInfoProvider,
	}

	mockShard1 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(1)).Return(mockShard1, serviceerror.NewShardOwnershipLost("", ""))

	_, err := h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 1,
	})
	assert.Error(t, err)
	var sol *serviceerror.ShardOwnershipLost
	assert.True(t, errors.As(err, &sol))

	mockShard2 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 2,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(2)).Return(mockShard2, nil)
	controller.EXPECT().ShardIDs().Return([]int32{2})
	namespaceRegistry.EXPECT().GetRegistrySize().Return(int64(0), int64(0))
	hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("0.0.0.0"))
	_, err = h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 2,
	})
	assert.NoError(t, err)
}
