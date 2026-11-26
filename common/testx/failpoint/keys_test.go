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

func (s *keySuite) TestNewKeyPanicsOnReservedPrefix() {
	prefix := failpoint.ReserveKeyPrefix[error]("test.reserved:")
	s.Equal(prefix, failpoint.ReserveKeyPrefix[error]("test.reserved:"))
	s.Panics(func() { failpoint.NewKey[error]("test.reserved:x") })
	s.Panics(func() { failpoint.ReserveKeyPrefix[error]("") })
	s.Equal("test.reserved:x", prefix.Key("x").Name())
	s.Equal("ns", prefix.Key("x").ForScope("ns").Scope())
}

// declaredFirst is declared once, whatever the number of runs of the test.
var declaredFirst = failpoint.NewKey[error]("test.declared-first.key")

func (s *keySuite) TestReserveKeyPrefixPanicsOnConflicts() {
	failpoint.ReserveKeyPrefix[int]("test.typed:")
	// One type per prefix, and prefixes that contain one another are ambiguous.
	s.Panics(func() { failpoint.ReserveKeyPrefix[error]("test.typed:") })
	s.Panics(func() { failpoint.ReserveKeyPrefix[int]("test.typed:sub.") })
	s.Panics(func() { failpoint.ReserveKeyPrefix[int]("test.") })
	// A declaration that already has the prefix conflicts, whatever the order of the initializers.
	s.Equal("test.declared-first.key", declaredFirst.Name())
	s.Panics(func() { failpoint.ReserveKeyPrefix[error]("test.declared-first.") })
}

func (s *keySuite) TestErrorKey() {
	prefix := failpoint.ReserveKeyPrefix[error]("test.errorkey:")
	failpoint.ReserveKeyPrefix[int]("test.intkey:")

	got, err := failpoint.ErrorKey("test.key1", "ns")
	s.NoError(err)
	s.Equal(key1.ForScope("ns"), got)
	got, err = failpoint.ErrorKey("test.errorkey:x", "")
	s.NoError(err)
	s.Equal(prefix.Key("x"), got)

	_, err = failpoint.ErrorKey("test.int", "")
	s.ErrorContains(err, `key "test.int" has the value type int, not error`)
	_, err = failpoint.ErrorKey("test.intkey:x", "")
	s.ErrorContains(err, "not error")
	_, err = failpoint.ErrorKey("test.undeclared", "")
	s.ErrorContains(err, `unknown key "test.undeclared"`)
	_, err = failpoint.ErrorKey("", "")
	s.ErrorContains(err, "unknown key")
}
