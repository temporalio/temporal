package configuredawait

import (
	"time"

	forwardawait "example.com/await"
	"go.temporal.io/testx/await"
	ps "go.temporal.io/testx/parallelsuite"
)

type testSuite struct{ ps.Suite[*testSuite] }

func awaiting(s *testSuite) {
	forwardawait.Requiref(s.Context(), s.TB(), func(*await.T) {}, time.Second, time.Millisecond, "waiting") // want "use suite Awaitf instead of await.Requiref"
}
