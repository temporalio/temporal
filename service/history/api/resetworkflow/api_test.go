package resetworkflow

import (
	"testing"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	resetWorkflowSuite struct {
		suite.Suite
	}
)

func TestResetWorkflowSuite(t *testing.T) {
	s := new(resetWorkflowSuite)
	suite.Run(t, s)
}

func (s *resetWorkflowSuite) TestShouldTolerateMissingCurrentExecution() {
	// No error resolving the current execution => not missing, no error.
	missing, err := shouldTolerateMissingCurrentExecution(nil, "base-run-id")
	s.False(missing)
	s.Require().NoError(err)

	notFound := serviceerror.NewNotFound("current execution not found")

	// NotFound with an explicit base runId => tolerate (proceed with no current), no error.
	missing, err = shouldTolerateMissingCurrentExecution(notFound, "base-run-id")
	s.True(missing)
	s.Require().NoError(err)

	// NotFound with an empty base runId => not tolerated (nothing to reset from), error returned.
	missing, err = shouldTolerateMissingCurrentExecution(notFound, "")
	s.False(missing)
	s.Require().ErrorIs(err, notFound)

	// A non-NotFound error => not tolerated regardless of base runId, error returned unchanged.
	other := serviceerror.NewUnavailable("persistence unavailable")
	missing, err = shouldTolerateMissingCurrentExecution(other, "base-run-id")
	s.False(missing)
	s.Require().ErrorIs(err, other)
}

func (s *resetWorkflowSuite) TestIsDuplicateResetRequest() {
	requestID := "request-id"
	testCases := []struct {
		name           string
		executionState *persistencespb.WorkflowExecutionState
		expected       bool
	}{
		{
			name: "legacy create request ID",
			executionState: &persistencespb.WorkflowExecutionState{
				CreateRequestId: requestID,
			},
			expected: true,
		},
		{
			name: "reset request marker",
			executionState: &persistencespb.WorkflowExecutionState{
				RequestIds: map[string]*persistencespb.RequestIDInfo{
					requestID: {
						EventType: enumspb.EVENT_TYPE_UNSPECIFIED,
						EventId:   common.EmptyEventID,
					},
				},
			},
			expected: true,
		},
		{
			name: "event-backed request ID",
			executionState: &persistencespb.WorkflowExecutionState{
				RequestIds: map[string]*persistencespb.RequestIDInfo{
					requestID: {
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
						EventId:   5,
					},
				},
			},
		},
		{
			name: "CHASM request ID",
			executionState: &persistencespb.WorkflowExecutionState{
				RequestIds: map[string]*persistencespb.RequestIDInfo{
					requestID: {
						AttachTime: timestamppb.Now(),
					},
				},
			},
		},
		{
			name:           "unknown request ID",
			executionState: &persistencespb.WorkflowExecutionState{},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.Equal(tc.expected, isDuplicateResetRequest(tc.executionState, requestID))
		})
	}
}

func (s *resetWorkflowSuite) TestGetResetReapplyExcludeTypes() {
	// Include all with no exclusions => no exclusions
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
	)
	// Include all with one exclusion => one exclusion (honor exclude in presence of default value of deprecated option)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {}},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE,
		),
	)
	// Include signal with no exclusions => exclude updates
	// (honor non-default value of deprecated option in presence of default value of non-deprecated option)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {}},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_SIGNAL,
		),
	)
	// Include signal with exclude signal => include signal means they want to exclude updates, and then the explicit
	// exclusion of signal trumps the deprecated inclusion
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_SIGNAL,
		),
	)
	// Include none with no exclusions => all excluded
	// (honor non-default value of deprecated option in presence of default value of non-deprecated option)
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{},
			enumspb.RESET_REAPPLY_TYPE_NONE,
		),
	)
	// Include none with exclude signal is all excluded
	s.Equal(
		map[enumspb.ResetReapplyExcludeType]struct{}{
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL: {},
			enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE: {},
		},
		GetResetReapplyExcludeTypes(
			[]enumspb.ResetReapplyExcludeType{enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL},
			enumspb.RESET_REAPPLY_TYPE_NONE,
		),
	)
}
