package executions

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
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
		shardID          int32
		executionManager persistence.ExecutionManager
	}
)

var _ Validator = (*historyEventIDValidator)(nil)

// NewHistoryEventIDValidator returns new instance.
func NewHistoryEventIDValidator(
	shardID int32,
	executionManager persistence.ExecutionManager,
) *historyEventIDValidator {
	return &historyEventIDValidator{
		shardID:          shardID,
		executionManager: executionManager,
	}
}

func (v *historyEventIDValidator) Validate(
	ctx context.Context,
	mutableState *MutableState,
) ([]MutableStateValidationResult, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return nil, err
	}

	if versionhistory.IsEmptyVersionHistory(currentVersionHistory) {
		return nil, nil
	}

	// TODO currently history event ID validator only verifies
	//  the first event batch exists, before doing whole history
	//  validation, ensure not too much capacity is consumed
	_, err = v.executionManager.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		MinEventID:    common.FirstEventID,
		MaxEventID:    common.FirstEventID + 1,
		BranchToken:   currentVersionHistory.BranchToken,
		ShardID:       v.shardID,
		PageSize:      1,
		NextPageToken: nil,
	})
	switch err.(type) {
	case nil:
		return nil, nil

	case *serviceerror.NotFound, *serviceerror.DataLoss:
		// additionally validate mutable state is still present in DB
		_, err = v.executionManager.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID:     v.shardID,
			NamespaceID: mutableState.GetExecutionInfo().NamespaceId,
			WorkflowID:  mutableState.GetExecutionInfo().WorkflowId,
			RunID:       mutableState.GetExecutionState().RunId,
			// TODO: for now only workflow has events and non-empty event version history
			// Later when supporting events for non-workflow component, we need to get ArchetypeID from MutableState.
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		switch err.(type) {
		case nil:
			return []MutableStateValidationResult{{
				failureType:    historyEventIDFailureType,
				failureDetails: historyEventIDFailureReason,
			}}, nil
		case *serviceerror.NotFound:
			// noop, mutable state is gone from DB
			// this can be the case during DB retention cleanup
			return nil, nil

		default:
			return nil, err
		}

	default:
		return nil, err
	}
}
