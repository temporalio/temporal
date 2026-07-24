package resetworkflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
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

func (s *resetWorkflowSuite) TestValidatePostResetOperationInputs_SignalWorkflow() {
	operations := []*workflowpb.PostResetOperation{{
		Variant: &workflowpb.PostResetOperation_SignalWorkflow_{
			SignalWorkflow: &workflowpb.PostResetOperation_SignalWorkflow{
				SignalName: "signal-name",
			},
		},
	}}

	skipReactivation, revisionNumbers, err := validatePostResetOperationInputs(
		context.Background(),
		operations,
		nil,
		nil,
		"task-queue",
		"namespace-id",
		100,
	)

	s.Require().NoError(err)
	s.Equal([]bool{false}, skipReactivation)
	s.Equal([]int64{0}, revisionNumbers)
}

func (s *resetWorkflowSuite) TestValidatePostResetOperationInputs_RejectsMalformedOperations() {
	testCases := []struct {
		name      string
		operation *workflowpb.PostResetOperation
		errText   string
	}{
		{
			name:      "nil operation",
			operation: nil,
			errText:   "unsupported post reset operation",
		},
		{
			name: "nil signal",
			operation: &workflowpb.PostResetOperation{
				Variant: &workflowpb.PostResetOperation_SignalWorkflow_{},
			},
			errText: "post reset signal workflow operation is not set",
		},
		{
			name: "nil options update",
			operation: &workflowpb.PostResetOperation{
				Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{},
			},
			errText: "post reset update workflow options operation is not set",
		},
		{
			name: "empty signal name",
			operation: &workflowpb.PostResetOperation{
				Variant: &workflowpb.PostResetOperation_SignalWorkflow_{
					SignalWorkflow: &workflowpb.PostResetOperation_SignalWorkflow{},
				},
			},
			errText: "SignalName is not set on request",
		},
		{
			name: "signal name too long",
			operation: &workflowpb.PostResetOperation{
				Variant: &workflowpb.PostResetOperation_SignalWorkflow_{
					SignalWorkflow: &workflowpb.PostResetOperation_SignalWorkflow{
						SignalName: "long-signal-name",
					},
				},
			},
			errText: "SignalName length exceeds limit",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			_, _, err := validatePostResetOperationInputs(
				context.Background(),
				[]*workflowpb.PostResetOperation{tc.operation},
				nil,
				nil,
				"task-queue",
				"namespace-id",
				10,
			)
			s.ErrorContains(err, tc.errText)
		})
	}
}
