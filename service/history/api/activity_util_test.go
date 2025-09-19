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
	ai := &persistencespb.ActivityInfo{StartedEventId: common.EmptyEventID}

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
	ai := &persistencespb.ActivityInfo{StartedEventId: common.EmptyEventID}

	completed := true
	// When completed-by-ID path, missing StartedEventId should not trigger not-found
	r.False(IsActivityTaskNotFoundForToken(token, ai, &completed))
}

func TestIsActivityTaskNotFoundForToken_AttemptMismatch(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := &tokenspb.Task{ScheduledEventId: 10, Attempt: 2}
	ai := &persistencespb.ActivityInfo{StartedEventId: 11, Attempt: 3}

	r.True(IsActivityTaskNotFoundForToken(token, ai, nil))

	// Match attempt -> not found should be false (no other mismatches)
	ai.Attempt = 2
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))
}

func TestIsActivityTaskNotFoundForToken_StartVersionChecks(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := &tokenspb.Task{StartVersion: 5}
	ai := &persistencespb.ActivityInfo{StartedEventId: 11, StartVersion: 6}

	// Both non-empty and different -> true
	r.True(IsActivityTaskNotFoundForToken(token, ai, nil))

	// Equal -> false
	ai.StartVersion = 5
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))

	// One side empty -> start-version check is skipped
	ai.StartVersion = common.EmptyVersion
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))
}

func TestIsActivityTaskNotFoundForToken_VersionChecks(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	token := &tokenspb.Task{Version: 7}
	ai := &persistencespb.ActivityInfo{StartedEventId: 11, Version: 8}

	r.True(IsActivityTaskNotFoundForToken(token, ai, nil))

	ai.Version = 7
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))

	// If token.Version is empty, version check is skipped -> false
	token.Version = common.EmptyVersion
	ai.Version = 12345
	r.False(IsActivityTaskNotFoundForToken(token, ai, nil))
}
