package adapter

import (
	"testing"
	ps "go.temporal.io/testx/parallelsuite"
)

func Run[T any](t *testing.T, s T) { t.Helper(); ps.Run(t, s) }
