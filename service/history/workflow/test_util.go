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

package workflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

func TestLocalMutableState(
	shard shard.Context,
	eventsCache events.Cache,
	ns *namespace.Namespace,
	workflowID string,
	runID string,
	logger log.Logger,
) *MutableStateImpl {

	ms := NewMutableState(shard, eventsCache, logger, ns, workflowID, runID, time.Now().UTC())
	ms.executionInfo.NamespaceId = string(ns.ID())
	ms.executionInfo.WorkflowId = workflowID
	ms.executionState.RunId = runID
	ms.GetExecutionInfo().ExecutionTime = ms.GetExecutionInfo().StartTime
	_ = ms.SetHistoryTree(nil, nil, runID)

	return ms
}

// NewMapEventCache is a functional event cache mock that wraps a simple Go map
func NewMapEventCache(
	t *testing.T,
	m map[events.EventKey]*historypb.HistoryEvent,
) events.Cache {
	cache := events.NewMockCache(gomock.NewController(t))
	cache.EXPECT().DeleteEvent(gomock.Any()).AnyTimes().Do(
		func(k events.EventKey) { delete(m, k) },
	)
	cache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes().Do(
		func(k events.EventKey, event *historypb.HistoryEvent) {
			m[k] = event
		},
	)
	cache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(
				_ context.Context,
				_ int32,
				key events.EventKey,
				_ int64,
				_ []byte,
			) (*historypb.HistoryEvent, error) {
				if event, ok := m[key]; ok {
					return event, nil
				}
				return nil, serviceerror.NewNotFound(fmt.Sprintf("event %#v not found", key))
			},
		)
	return cache
}

func TestGlobalMutableState(
	shard shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	version int64,
	workflowID string,
	runID string,
) *MutableStateImpl {

	ms := NewMutableState(shard, eventsCache, logger, tests.GlobalNamespaceEntry, workflowID, runID, time.Now().UTC())
	ms.GetExecutionInfo().ExecutionTime = ms.GetExecutionInfo().StartTime
	_ = ms.UpdateCurrentVersion(version, false)
	_ = ms.SetHistoryTree(nil, nil, runID)

	return ms
}

func TestCloneToProto(
	mutableState MutableState,
) *persistencespb.WorkflowMutableState {
	if mutableState.HasBufferedEvents() {
		_, _, _ = mutableState.CloseTransactionAsMutation(TransactionPolicyActive)
	} else {
		_, _, _ = mutableState.CloseTransactionAsSnapshot(TransactionPolicyActive)
	}
	return mutableState.CloneToProto()
}
