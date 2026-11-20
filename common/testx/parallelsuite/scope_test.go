package parallelsuite_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
	const timeout = time.Second
	checks := map[string]func(parallelsuite.Scope){
		"Containsf": func(s parallelsuite.Scope) { s.Containsf([]string{"pending"}, "ready", "state %s is missing", "ready") },
		"Failf":     func(s parallelsuite.Scope) { s.Failf("not ready", "current state is %s", "pending") },
		"NoError":   func(s parallelsuite.Scope) { s.NoError(errors.New("not ready")) },
		"Equal":     func(s parallelsuite.Scope) { s.Equal("ready", "pending") },
		"EqualValuesf": func(s parallelsuite.Scope) {
			s.EqualValuesf(1, int64(2), "unexpected count %d", 2)
		},
		"Greater":        func(s parallelsuite.Scope) { s.Greater(1, 2) },
		"GreaterOrEqual": func(s parallelsuite.Scope) { s.GreaterOrEqual(1, 2) },
		"GreaterOrEqualf": func(s parallelsuite.Scope) {
			s.GreaterOrEqualf(1, 2, "expected at least %d", 2)
		},
		"Len":         func(s parallelsuite.Scope) { s.Len([]int{1}, 2) },
		"LessOrEqual": func(s parallelsuite.Scope) { s.LessOrEqual(2, 1) },
		"Nil":         func(s parallelsuite.Scope) { s.Nil("pending") },
		"NotNil":      func(s parallelsuite.Scope) { s.NotNil(nil) },
		"NotEmpty":    func(s parallelsuite.Scope) { s.NotEmpty("") },
		"True":        func(s parallelsuite.Scope) { s.True(false) },
	}
	for name, check := range checks {
		s.Run(name, func(s *scopeAssertionsSuite) {
			type contextKey struct{}
			ctx := context.WithValue(s.Context(), contextKey{}, name)
			scope := parallelsuite.WithContext(ctx, s)
			attempts := 0
			completed := 0
			parallelsuite.Await(scope, func(s parallelsuite.Scope) {
				attempts++
				if attempts == 1 {
					check(s)
				}
				checkScopeAssertions(s)
				s.Equal(name, s.Context().Value(contextKey{}))
				deadline, ok := s.Context().Deadline()
				s.True(ok, "await context must have a deadline")
				remaining := time.Until(deadline)
				s.True(remaining <= timeout, "await deadline is %v away; want at most %v", remaining, timeout)
				completed++
			}, timeout, time.Millisecond)
			s.Equal(2, attempts)
			s.Equal(1, completed)
		})
	}
}
