// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/parallelsuite.
package parallelsuite

import (
	"context"
	"testing"

	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/testx/parallelsuite"
)

type Scope = parallelsuite.Scope

// Suite extends the shared parallel suite with server assertions.
type Suite[T parallelsuite.TestingSuite] struct {
	parallelsuite.Suite[T]
	protorequire.ProtoAssertions
	historyrequire.HistoryRequire
}

// InitAssertions binds server assertions to each test, subtest, or await attempt.
func (s *Suite[T]) InitAssertions() {
	s.ProtoAssertions = protorequire.New(s.AssertionT())
	s.HistoryRequire = historyrequire.New(s.AssertionT())
}

func WithContext(ctx context.Context, scope Scope) Scope {
	return parallelsuite.WithContext(ctx, scope)
}

func Run[T parallelsuite.TestingSuite](t *testing.T, s T, args ...any) {
	t.Helper()
	parallelsuite.Run(t, s, args...)
}

// Deprecated: use [Run] for new tests. This only exists for backwards-compatibility
// with legacy behavior to ease migration.
func RunLegacySequential[T parallelsuite.TestingSuite](t *testing.T, s T, args ...any) {
	t.Helper()
	parallelsuite.RunLegacySequential(t, s, args...) //nolint:staticcheck // SA1019: forwarding the legacy runner
}
