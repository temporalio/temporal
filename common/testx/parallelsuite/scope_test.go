package parallelsuite_test

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/testx/await"
	"go.temporal.io/testx/parallelsuite"
)

type scopeAssertionsSuite struct {
	parallelsuite.Suite[*scopeAssertionsSuite]
}

func TestScopeAssertions(t *testing.T) {
	parallelsuite.Run(t, &scopeAssertionsSuite{})
}

func checkScopeAssertions(s parallelsuite.Scope) {
	s.NoError(nil)
	s.Equal("ready", "ready")
	s.Nil(nil)
	s.NotNil(s.Context())
	s.NotEmpty("ready")
	s.True(true)
}

func (s *scopeAssertionsSuite) TestSuiteHelpers() {
	checkScopeAssertions(parallelsuite.WithContext(s.Context(), s))
}

func (s *scopeAssertionsSuite) TestAwaitHelpersRetryFailedAssertions() {
	checks := map[string]func(parallelsuite.Scope){
		"NoError":  func(s parallelsuite.Scope) { s.NoError(errors.New("not ready")) },
		"Equal":    func(s parallelsuite.Scope) { s.Equal("ready", "pending") },
		"Nil":      func(s parallelsuite.Scope) { s.Nil("pending") },
		"NotNil":   func(s parallelsuite.Scope) { s.NotNil(nil) },
		"NotEmpty": func(s parallelsuite.Scope) { s.NotEmpty("") },
		"True":     func(s parallelsuite.Scope) { s.True(false) },
	}
	for name, check := range checks {
		s.Run(name, func(s *scopeAssertionsSuite) {
			attempts := 0
			completed := 0
			await.Require(s.Context(), s.T(), func(t *await.T) {
				attempts++
				scope := parallelsuite.WithContext(t.Context(), t)
				if attempts == 1 {
					check(scope)
				}
				checkScopeAssertions(scope)
				completed++
			}, time.Second, time.Millisecond)
			s.Equal(2, attempts)
			s.Equal(1, completed)
		})
	}
}
