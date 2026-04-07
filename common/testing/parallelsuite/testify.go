package parallelsuite

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testifysuite "github.com/stretchr/testify/suite"
)

// testifyBase wraps testify's suite.Suite so that Suite's *require.Assertions
// shadows testify's *assert.Assertions in Go's embedding resolution.
// It disables all testify suite methods with panicking stubs.
type testifyBase struct {
	testifysuite.Suite
}

// Deprecated: SetT is managed internally by [Run]. Do not call directly.
func (b *testifyBase) SetT(_ *testing.T) {
	panic("parallelsuite: do not call SetT directly; it is managed by parallelsuite.Run")
}

// Deprecated: SetS is managed internally by [Run]. Do not call directly.
func (b *testifyBase) SetS(_ testifysuite.TestingSuite) {
	panic("parallelsuite: do not call SetS directly; it is managed by parallelsuite.Run")
}

// Deprecated: Assert returns non-fatal assertions which are not supported.
// Use s.NoError, s.Equal, etc. directly (require semantics).
func (b *testifyBase) Assert() *assert.Assertions {
	panic("parallelsuite: do not use Assert(); use s.NoError, s.Equal, etc. directly")
}

// Deprecated: Require bypasses the guard mechanism. Use s.NoError, s.Equal, etc. directly.
func (b *testifyBase) Require() *require.Assertions {
	panic("parallelsuite: do not use Require(); use s.NoError, s.Equal, etc. directly")
}
