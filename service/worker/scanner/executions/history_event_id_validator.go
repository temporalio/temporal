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

package executions

import (
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
)

const (
	historyEventIDFailureType   = "history_event_id_validator"
	historyEventIDFailureReason = "execution missing first event batch"
)

type (
	// historyEventIDValidator is a validator that checks event IDs are contiguous
	historyEventIDValidator struct {
		shardID        int32
		historyManager persistence.HistoryManager
	}
)

var _ Validator = (*historyEventIDValidator)(nil)

// NewEventIDValidator returns new instance.
func NewHistoryEventIDValidator(
	shardID int32,
	historyManager persistence.HistoryManager,
) *historyEventIDValidator {
	return &historyEventIDValidator{
		shardID:        shardID,
		historyManager: historyManager,
	}
}

func (v *historyEventIDValidator) Validate(
	mutableState *MutableState,
) ([]MutableStateValidationResult, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return nil, err
	}

	// TODO currently history event ID validator only verifies
	//  the first event batch exists, before doing whole history
	//  validation, ensure not too much capacity is consumed
	_, err = v.historyManager.ReadRawHistoryBranch(&persistence.ReadHistoryBranchRequest{
		MinEventID:    common.FirstEventID,
		MaxEventID:    common.FirstEventID + 1,
		BranchToken:   currentVersionHistory.BranchToken,
		ShardID:       convert.Int32Ptr(v.shardID),
		PageSize:      1,
		NextPageToken: nil,
	})
	switch err.(type) {
	case nil:
		return nil, nil
	case *serviceerror.NotFound, *serviceerror.DataLoss:
		return []MutableStateValidationResult{{
			failureType:    historyEventIDFailureType,
			failureDetails: historyEventIDFailureReason,
		}}, nil
	default:
		return nil, err
	}
}
