package rule

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/tests/umpire2/fact"
)

func TestNexusActivityLinkConsistencyAcceptsMatchingPair(t *testing.T) {
	state := newTestModelState()
	routeFact(t, state, fact.NewNexusOperationExecutionSnapshot("namespace-id", "operation-id", []*commonpb.Link{activityLink("activity-id")}))
	routeFact(t, state, fact.NewActivityExecutionSnapshot("namespace-id", "activity-id", enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, []*commonpb.Link{nexusLink("operation-id")}))

	require.Empty(t, checkSafetyRule(state, &NexusActivityLinkConsistency{}))
}

func TestNexusActivityLinkConsistencyWaitsForLinkedEntityObservation(t *testing.T) {
	state := newTestModelState()
	routeFact(t, state, fact.NewNexusOperationExecutionSnapshot("namespace-id", "operation-id", []*commonpb.Link{activityLink("activity-id")}))

	require.Empty(t, checkSafetyRule(state, &NexusActivityLinkConsistency{}))
}

func TestNexusActivityLinkConsistencyRejectsMissingActivityBackLink(t *testing.T) {
	state := newTestModelState()
	routeFact(t, state, fact.NewNexusOperationExecutionSnapshot("namespace-id", "operation-id", []*commonpb.Link{activityLink("activity-id")}))
	routeFact(t, state, fact.NewActivityExecutionSnapshot("namespace-id", "activity-id", enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, nil))

	violations := checkSafetyRule(state, &NexusActivityLinkConsistency{})
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "no matching activity back-link")
}

func TestNexusActivityLinkConsistencyRejectsMissingNexusLink(t *testing.T) {
	state := newTestModelState()
	routeFact(t, state, fact.NewNexusOperationExecutionSnapshot("namespace-id", "operation-id", nil))
	routeFact(t, state, fact.NewActivityExecutionSnapshot("namespace-id", "activity-id", enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, []*commonpb.Link{nexusLink("operation-id")}))

	violations := checkSafetyRule(state, &NexusActivityLinkConsistency{})
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "no matching Nexus-side link")
}

func activityLink(activityID string) *commonpb.Link {
	return &commonpb.Link{Variant: &commonpb.Link_Activity_{Activity: &commonpb.Link_Activity{ActivityId: activityID}}}
}

func nexusLink(operationID string) *commonpb.Link {
	return &commonpb.Link{Variant: &commonpb.Link_NexusOperation_{NexusOperation: &commonpb.Link_NexusOperation{OperationId: operationID}}}
}
