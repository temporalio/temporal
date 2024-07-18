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

package replicator

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
)

func TestCleanupAckedMessages_GetAckLevelsError(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(nil, fmt.Errorf("test"))
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), gomock.Any()).Times(0)
	_, err := replicator.cleanupAckedMessages(context.Background(), 1)

	assert.Error(t, err)
}

func TestCleanupAckedMessages_EmptyGetAckLevels(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(nil, nil)
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), int64(1)).Times(0)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(nil)
	mid, err := replicator.cleanupAckedMessages(context.Background(), 1)

	assert.Equal(t, int64(1), mid)
	assert.NoError(t, err)

}

func TestCleanupAckedMessages_Noop(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(map[string]int64{
		"a": 2,
		"b": 3,
		"c": 5,
		"d": 10,
		"e": 1,
	}, nil)
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), int64(1)).Times(0)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"b": {},
		"c": {},
		"d": {},
		"e": {},
	})
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("e").AnyTimes()
	mid, err := replicator.cleanupAckedMessages(context.Background(), 5)

	assert.Equal(t, int64(5), mid)
	assert.NoError(t, err)
}

func TestCleanupAckedMessages_DeleteMessage(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(map[string]int64{
		"a": 2,
		"b": 3,
		"c": 5,
		"d": 10,
		"e": 1,
	}, nil)
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), int64(10)).Times(1)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"d": {},
		"e": {},
	})
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("e").AnyTimes()
	mid, err := replicator.cleanupAckedMessages(context.Background(), 5)

	assert.Equal(t, int64(10), mid)
	assert.NoError(t, err)
}
