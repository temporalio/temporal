package failpoint_test

import (
	"crypto/rand"
	"errors"
	"testing"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/parallelsuite"
)

type errorCatalogSuite struct {
	parallelsuite.Suite[*errorCatalogSuite]
}

func TestErrorCatalogSuite(t *testing.T) {
	parallelsuite.Run(t, &errorCatalogSuite{})
}

func (s *errorCatalogSuite) TestErrorCatalog() {
	injected := errors.New("base")
	// The catalog is global, so each run registers a name of its own.
	name := "TestErrorCatalog_" + rand.Text()
	failpoint.RegisterError(name, func(msg string) error {
		return errors.Join(injected, errors.New(msg))
	})

	newError, ok := failpoint.LookupError(name)
	s.True(ok)
	err := newError("detail")
	s.ErrorIs(err, injected)
	s.ErrorContains(err, "detail")

	_, ok = failpoint.LookupError(name + "_Unknown")
	s.False(ok)

	s.Contains(failpoint.ErrorNames(), name)
	s.Panics(func() {
		failpoint.RegisterError(name, func(string) error { return nil })
	})
}
