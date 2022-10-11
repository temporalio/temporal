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

package api

import (
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/workflow"
)

func IsRetryableError(err error) bool {
	return err == consts.ErrStaleState ||
		err == consts.ErrLocateCurrentWorkflowExecution ||
		err == consts.ErrBufferedQueryCleared ||
		persistence.IsConflictErr(err) ||
		common.IsServiceHandlerRetryableError(err)
}

func IsHistoryEventOnCurrentBranch(
	mutableState workflow.MutableState,
	eventID int64,
	eventVersion int64,
) (bool, error) {
	if eventVersion == 0 {
		if eventID >= mutableState.GetNextEventID() {
			return false, &serviceerror.NotFound{Message: "History event not found"}
		}

		// there's no version, assume the event is on the current branch
		return true, nil
	}

	versionHistoryies := mutableState.GetExecutionInfo().GetVersionHistories()
	versionHistoryItem := versionhistory.NewVersionHistoryItem(eventID, eventVersion)
	if _, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(
		versionHistoryies,
		versionHistoryItem,
	); err != nil {
		return false, &serviceerror.NotFound{Message: "History event not found"}
	}

	// check if on current branch
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistoryies)
	if err != nil {
		return false, err
	}

	return versionhistory.ContainsVersionHistoryItem(
		currentVersionHistory,
		versionHistoryItem,
	), nil
}
