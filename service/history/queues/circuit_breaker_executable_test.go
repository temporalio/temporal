package queues_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/queues"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
)

// recordingBreaker records the order in which outcomes are reported to it.
type recordingBreaker struct {
	events []string
}

func (b *recordingBreaker) Name() string             { return "test" }
func (b *recordingBreaker) State() gobreaker.State   { return gobreaker.StateClosed }
func (b *recordingBreaker) Counts() gobreaker.Counts { return gobreaker.Counts{} }
func (b *recordingBreaker) Allow() (func(bool), error) {
	return func(success bool) { b.events = append(b.events, fmt.Sprintf("done:%v", success)) }, nil
}
func (b *recordingBreaker) RecordFailure(message string) {
	b.events = append(b.events, "failure:"+message)
}
func (b *recordingBreaker) RecordSuccess() { b.events = append(b.events, "success") }

func TestCircuitBreakerExecutable_RecordsOutcomeBeforeCompleting(t *testing.T) {
	for _, tc := range []struct {
		name       string
		executeErr error
		want       []string
	}{
		{
			name:       "destination down",
			executeErr: queueserrors.NewDestinationDownError("connection refused", nil),
			want:       []string{"failure:destination down: connection refused", "done:false"},
		},
		{name: "success", want: []string{"success", "done:true"}},
		{name: "other error", executeErr: errors.New("internal"), want: []string{"success", "done:true"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			inner := queues.NewMockExecutable(ctrl)
			inner.EXPECT().Execute().Return(tc.executeErr)
			breaker := &recordingBreaker{}

			_ = queues.NewCircuitBreakerExecutable(inner, breaker, metrics.NoopMetricsHandler).Execute()

			require.Equal(t, tc.want, breaker.events)
		})
	}
}
