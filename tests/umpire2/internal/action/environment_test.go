package action_test

import (
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire2/internal/action"
)

var _ action.Environment = (*testcore.TestEnv)(nil)
