package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
)

func TestIsActivityTaskNotFoundForToken_NotStarted_ReturnsTrue(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := &tokenspb.Task{}
	ai := persistencespb.ActivityInfo_builder{StartedEventId: common.EmptyEventID}.Build()

	// isCompletedByID unset -> treat as false
	r.True(IsActivityTaskNotFoundForToken(token, ai, nil))

	// isCompletedByID = false -> still true if not started
	completed := false
	r.True(IsActivityTaskNotFoundForToken(token, ai, &completed))
}

func TestIsActivityTaskNotFoundForToken_NotStarted_CompletedByID_BypassesCheck(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := &tokenspb.Task{}
	ai := persistencespb.ActivityInfo_builder{StartedEventId: common.EmptyEventID}.Build()

	completed := true
	// When completed-by-ID path, missing StartedEventId should not trigger not-found
	r.False(IsActivityTaskNotFoundForToken(token, ai, &completed))
}

func TestIsActivityTaskNotFoundForToken_AttemptMismatch(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := tokenspb.Task_builder{ScheduledEventId: 10, Attempt: 2}.Build()
	ai := persistencespb.ActivityInfo_builder{StartedEventId: 11, Attempt: 3}.Build()

	r.True(IsActivityTaskNotFoundForToken(token, ai, nil))

	// Match attempt -> not found should be false (no other mismatches)
	ai.SetAttempt(2)
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))
}

func TestIsActivityTaskNotFoundForToken_StartVersionChecks(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := tokenspb.Task_builder{StartVersion: 5}.Build()
	ai := persistencespb.ActivityInfo_builder{StartedEventId: 11, StartVersion: 6}.Build()

	// Both non-empty and different -> true
	r.True(IsActivityTaskNotFoundForToken(token, ai, nil))

	// Equal -> false
	ai.SetStartVersion(5)
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))

	// One side empty -> start-version check is skipped
	ai.SetStartVersion(common.EmptyVersion)
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))
}

func TestIsActivityTaskNotFoundForToken_VersionChecks(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := tokenspb.Task_builder{Version: 7}.Build()
	ai := persistencespb.ActivityInfo_builder{StartedEventId: 11, Version: 8}.Build()

	r.True(IsActivityTaskNotFoundForToken(token, ai, nil))

	ai.SetVersion(7)
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))

	// If token.Version is empty, version check is skipped -> false
	token.SetVersion(common.EmptyVersion)
	ai.SetVersion(12345)
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))
}
