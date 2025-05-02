package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
)

func TestNewRealClock_Now(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	location := source.Now().Location()
	assert.Equal(t, "UTC", location.String())
}

func TestNewRealClock_Since(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	start := source.Now()
	assert.Eventually(
		t,
		func() bool {
			return source.Since(start) >= 5*time.Millisecond
		},
		time.Second,
		time.Millisecond,
	)
}

func TestNewRealClock_AfterFunc(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	ch := make(chan struct{})
	timer := source.AfterFunc(0, func() {
		close(ch)
	})

	<-ch
	assert.False(t, timer.Stop())
}

func TestNewRealClock_NewTimer(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	ch, timer := source.NewTimer(0)
	<-ch
	assert.False(t, timer.Stop())
}

func TestNewRealClock_NewTimer_Stop(t *testing.T) {
	t.Parallel()

	source := clock.NewRealTimeSource()
	_, timer := source.NewTimer(time.Second)
	assert.True(t, timer.Stop())
}
