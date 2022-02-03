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
	"time"

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
	logger log.Logger,
	runID string,
) *MutableStateImpl {

	msBuilder := NewMutableState(shard, eventsCache, logger, ns, time.Now().UTC())
	msBuilder.GetExecutionInfo().ExecutionTime = msBuilder.GetExecutionInfo().StartTime
	_ = msBuilder.SetHistoryTree(runID)

	return msBuilder
}

func TestGlobalMutableState(
	shard shard.Context,
	eventsCache events.Cache,
	logger log.Logger,
	version int64,
	runID string,
) *MutableStateImpl {

	msBuilder := NewMutableState(shard, eventsCache, logger, tests.GlobalNamespaceEntry, time.Now().UTC())
	msBuilder.GetExecutionInfo().ExecutionTime = msBuilder.GetExecutionInfo().StartTime
	_ = msBuilder.UpdateCurrentVersion(version, false)
	_ = msBuilder.SetHistoryTree(runID)

	return msBuilder
}

func TestCloneToProto(
	mutableState MutableState,
) *persistencespb.WorkflowMutableState {
	if mutableState.HasBufferedEvents() {
		_, _, _ = mutableState.CloseTransactionAsMutation(time.Now().UTC(), TransactionPolicyActive)
	} else {
		_, _, _ = mutableState.CloseTransactionAsSnapshot(time.Now().UTC(), TransactionPolicyActive)
	}
	return mutableState.CloneToProto()
}
