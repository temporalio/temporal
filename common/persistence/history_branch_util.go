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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination history_branch_util_mock.go

package persistence

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
)

type (
	HistoryBranchUtil interface {
		NewHistoryBranch(
			namespaceID string,
			workflowID string,
			runID string,
			treeID string,
			branchID *string,
			ancestors []*persistencespb.HistoryBranchRange,
			runTimeout time.Duration,
			executionTimeout time.Duration,
			retentionDuration time.Duration,
		) ([]byte, error)
		// ParseHistoryBranchInfo parses the history branch for branch information
		ParseHistoryBranchInfo(branchToken []byte) (*persistencespb.HistoryBranch, error)
		// UpdateHistoryBranchInfo updates the history branch with branch information
		UpdateHistoryBranchInfo(branchToken []byte, branchInfo *persistencespb.HistoryBranch) ([]byte, error)
	}

	HistoryBranchUtilImpl struct {
	}
)

func NewHistoryBranch(
	treeID string,
	branchID *string,
	ancestors []*persistencespb.HistoryBranchRange,
) ([]byte, error) {
	var id string
	if branchID == nil {
		id = primitives.NewUUID().String()
	} else {
		id = *branchID
	}
	bi := &persistencespb.HistoryBranch{
		TreeId:    treeID,
		BranchId:  id,
		Ancestors: ancestors,
	}
	data, err := serialization.HistoryBranchToBlob(bi)
	if err != nil {
		return nil, err
	}
	return data.Data, nil
}

func (u *HistoryBranchUtilImpl) NewHistoryBranch(
	namespaceID string,
	workflowID string,
	runID string,
	treeID string,
	branchID *string,
	ancestors []*persistencespb.HistoryBranchRange,
	runTimeout time.Duration,
	executionTimeout time.Duration,
	retentionDuration time.Duration,
) ([]byte, error) {
	return NewHistoryBranch(treeID, branchID, ancestors)
}

func (u *HistoryBranchUtilImpl) ParseHistoryBranchInfo(branchToken []byte) (*persistencespb.HistoryBranch, error) {
	return serialization.HistoryBranchFromBlob(branchToken, enumspb.ENCODING_TYPE_PROTO3.String())
}

func (u *HistoryBranchUtilImpl) UpdateHistoryBranchInfo(branchToken []byte, branchInfo *persistencespb.HistoryBranch) ([]byte, error) {
	bi, err := serialization.HistoryBranchFromBlob(branchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, err
	}
	bi.TreeId = branchInfo.TreeId
	bi.BranchId = branchInfo.BranchId
	bi.Ancestors = branchInfo.Ancestors

	blob, err := serialization.HistoryBranchToBlob(bi)
	if err != nil {
		return nil, err
	}
	return blob.Data, nil
}

func (u *HistoryBranchUtilImpl) GetHistoryBranchUtil() HistoryBranchUtil {
	return u
}
