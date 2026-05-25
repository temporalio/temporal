package activity

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

// TestStartActivityExecution_RejectsLinksOverExecutionLimit verifies that the
// initial-create path enforces MaxLinksPerExecution before persisting the
// activity, preventing inconsistent state where later attachLinks calls would
// fail because the activity was seeded with more links than the cap.
func TestStartActivityExecution_RejectsLinksOverExecutionLimit(t *testing.T) {
	const maxLinks = 3
	h := &handler{
		config: &Config{
			MaxCallbacksPerExecution: func(string) int { return 10 },
			MaxLinksPerExecution:     func(string) int { return maxLinks },
		},
	}

	links := make([]*commonpb.Link, maxLinks+1)
	for i := range links {
		links[i] = &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  "test-namespace",
					WorkflowId: fmt.Sprintf("wf-%d", i),
				},
			},
		}
	}

	_, err := h.StartActivityExecution(context.Background(), &activitypb.StartActivityExecutionRequest{
		NamespaceId: "test-namespace-id",
		FrontendRequest: &workflowservice.StartActivityExecutionRequest{
			Namespace:          "test-namespace",
			ActivityId:         "test-activity-id",
			IdReusePolicy:      enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			IdConflictPolicy:   enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL,
			Links:              links,
		},
	})

	require.Error(t, err)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	require.EqualError(t, err, fmt.Sprintf(
		"cannot attach more than %d links to an activity (%d links already attached)",
		maxLinks, 0,
	))
}
