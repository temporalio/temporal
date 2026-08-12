package action

import "go.temporal.io/server/tests/testcore"

var _ Environment = (*testcore.TestEnv)(nil)
