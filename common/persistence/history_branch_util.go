//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination history_branch_util_mock.go

package persistence

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
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
		ParseHistoryBranchInfo(
			branchToken []byte,
		) (*persistencespb.HistoryBranch, error)
		// UpdateHistoryBranchInfo updates the history branch with branch information
		UpdateHistoryBranchInfo(
			branchToken []byte,
			branchInfo *persistencespb.HistoryBranch,
			runID string,
		) ([]byte, error)
	}

	HistoryBranchUtilImpl struct {
	}
)

func (u *HistoryBranchUtilImpl) NewHistoryBranch(
	_ string, // namespaceID
	_ string, // workflowID
	_ string, // runID
	treeID string,
	branchID *string,
	ancestors []*persistencespb.HistoryBranchRange,
	_ time.Duration, // runTimeout
	_ time.Duration, // executionTimeout
	_ time.Duration, // retentionDuration
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

func (u *HistoryBranchUtilImpl) ParseHistoryBranchInfo(
	branchToken []byte,
) (*persistencespb.HistoryBranch, error) {
	return serialization.HistoryBranchFromBlob(&commonpb.DataBlob{Data: branchToken, EncodingType: enumspb.ENCODING_TYPE_PROTO3})
}

func (u *HistoryBranchUtilImpl) UpdateHistoryBranchInfo(
	branchToken []byte,
	branchInfo *persistencespb.HistoryBranch,
	runID string,
) ([]byte, error) {
	bi, err := serialization.HistoryBranchFromBlob(&commonpb.DataBlob{Data: branchToken, EncodingType: enumspb.ENCODING_TYPE_PROTO3})
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
