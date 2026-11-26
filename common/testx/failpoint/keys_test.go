package failpoint_test

import (
	"testing"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/parallelsuite"
)

type keySuite struct {
	parallelsuite.Suite[*keySuite]
}

func TestKeySuite(t *testing.T) {
	parallelsuite.Run(t, &keySuite{})
}

func (s *keySuite) TestKey() {
	scoped := key1.ForScope("ns-1")
	s.Equal("test.key1", key1.Name())
	s.Empty(key1.Scope())
	s.Equal("test.key1", key1.String())
	s.Equal("test.key1", scoped.Name())
	s.Equal("ns-1", scoped.Scope())
	s.Equal("test.key1@ns-1", scoped.String())
	s.NotEqual(key1, scoped)
}

func (s *keySuite) TestKeyComparableForNonComparableType() {
	k := failpoint.UndeclaredKey[func()]("test.func")
	m := map[failpoint.Key[func()]]int{k: 1}
	s.Equal(1, m[k])
	s.NotEqual(k, k.ForScope("s"))
}

func (s *keySuite) TestNewKeyPanicsOnDuplicateName() {
	s.Panics(func() { failpoint.NewKey[error]("test.key1") })
}

func (s *keySuite) TestNewKeyPanicsOnEmptyName() {
	s.Panics(func() { failpoint.NewKey[error]("") })
}
