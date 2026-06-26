package tasktoken

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestStandaloneActivityTokenMatchesPollToken verifies that NewStandaloneActivityTaskToken
// produces a token byte-identical to what matching builds via NewActivityTaskToken during poll.
// Matching uses NewActivityTaskToken with scheduledEventId=0, clock=nil, version=0, startVersion=0
// for standalone activities (see matching_engine.go createPollActivityTaskQueueResponse).
// If NewActivityTaskToken gains new fields, this test will fail, reminding us to update
// NewStandaloneActivityTaskToken to match.
func TestStandaloneActivityTokenMatchesPollToken(t *testing.T) {
	namespaceID := "ns-id"
	workflowID := "wf-id"
	runID := "run-id"
	activityID := "act-id"
	activityType := "MyActivity"
	attempt := int32(3)
	componentRef := []byte("some-component-ref")

	// This is what matching builds for a standalone activity poll response.
	pollToken := NewActivityTaskToken(
		namespaceID, workflowID, runID,
		0, // scheduledEventId — always 0 for standalone
		activityID, activityType, attempt,
		nil, // clock — always nil for standalone
		0,   // version — always 0 for standalone
		0,   // startVersion — always 0 for standalone
		componentRef,
	)

	// This is what the cancel command handler builds.
	cancelToken := NewStandaloneActivityTaskToken(
		namespaceID, workflowID, runID,
		activityID, activityType, attempt,
		componentRef,
	)

	pollBytes, err := proto.Marshal(pollToken)
	require.NoError(t, err)
	cancelBytes, err := proto.Marshal(cancelToken)
	require.NoError(t, err)

	require.Equal(t, pollBytes, cancelBytes,
		"cancel command token must be byte-identical to poll token")
}
