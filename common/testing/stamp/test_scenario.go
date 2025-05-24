package stamp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
)

var (
	_                      testEnv = (*Scenario)(nil)
	defaultScenarioTimeout         = 5 * time.Second
	defaultWaiterTimeout           = 2 * time.Second
	defaultWaiterPolling           = 100 * time.Millisecond
)

type (
	Scenario struct {
		t           *testing.T
		logger      log.Logger
		require     *require.Assertions
		genCtx      *genContext
		timeout     time.Duration
		ctx         context.Context
		ctxCancelFn context.CancelFunc
		happyCase   bool
	}
	waiter struct {
		timeout time.Duration
		polling time.Duration
	}
	ScenarioOption interface {
		applyScenario(*Scenario)
	}
	WaiterOption interface {
		applyWaiter(*waiter)
	}
)

type timeoutOption struct {
	duration time.Duration
}

func (o timeoutOption) applyScenario(s *Scenario) {
	s.timeout = o.duration
}

func (o timeoutOption) applyWaiter(w *waiter) {
	w.timeout = o.duration
}

// WithTimeout is an option to set a timeout.
func WithTimeout(timeout time.Duration) timeoutOption {
	return timeoutOption{duration: timeout}
}

type pollingOption struct {
	interval time.Duration
}

func (o pollingOption) applyWaiter(w *waiter) {
	w.polling = o.interval
}

// WithPolling is an option to set a polling interval for a waiter.
func WithPolling(interval time.Duration) WaiterOption {
	return pollingOption{interval: interval}
}

type happyCaseOption struct{}

func (o happyCaseOption) applyScenario(s *Scenario) {
	s.happyCase = true
}

type allowRandomOption struct{}

func (o allowRandomOption) applyScenario(s *Scenario) {
	s.genCtx.allowRandom = true
}

// HappyCase is an option to mark the scenario as a happy case.
// A happy case scenario is run before all other scenarios to ensure
// that the basic functionality is working as expected.
// TODO: implement
func HappyCase() ScenarioOption {
	return happyCaseOption{}
}

type seedOption struct {
	seed int
}

func (o seedOption) applyScenario(s *Scenario) {
	s.genCtx.baseSeed = o.seed
}

func WithSeed(seed int) ScenarioOption {
	return seedOption{
		seed: seed,
	}
}

func newScenario(
	t *testing.T,
	logger log.Logger,
	opts ...ScenarioOption,
) *Scenario {
	t.Helper()

	s := &Scenario{
		t:       t,
		logger:  logger,
		genCtx:  newGenContext(newSeed(t.Name())),
		require: require.New(t),
		timeout: defaultScenarioTimeout,
	}
	for _, opt := range opts {
		opt.applyScenario(s)
	}

	s.ctx, s.ctxCancelFn = context.WithTimeout(context.Background(), s.timeout)
	return s
}

func (s *Scenario) Logger() log.Logger {
	return s.logger
}

func (s *Scenario) Context() context.Context {
	return s.ctx
}

func (s *Scenario) Verify(f func() Prop[bool], opts ...WaiterOption) {
	s.t.Helper()

	res, _, err := f().get()
	s.require.NoError(err)
	if res != true {
		s.require.FailNow("property invalid")
	}
}

func (s *Scenario) Eventually(f func() Prop[bool], opts ...WaiterOption) {
	s.t.Helper()

	w := &waiter{
		timeout: defaultWaiterTimeout,
		polling: defaultWaiterPolling,
	}
	for _, opt := range opts {
		opt.applyWaiter(w)
	}

	waiterCtx, cancelFn := context.WithTimeout(context.Background(), w.timeout*debug.TimeoutMultiplier)
	defer cancelFn()

	ticker := time.NewTicker(w.polling)
	defer ticker.Stop()

	prop := f()

	var lastEvalCtx *PropContext
	var errMsg string
loop:
	for {
		select {
		case <-ticker.C:
			res, evalCtx, err := prop.get()
			s.require.NoError(err)
			if res == true {
				return
			}
			lastEvalCtx = evalCtx
		case <-s.ctx.Done():
			if ctxErr := s.ctx.Err(); errors.Is(ctxErr, context.DeadlineExceeded) {
				errMsg = fmt.Sprintf("Eventually aborted since scenario timed out after %s\n\n", s.timeout)
			} else {
				errMsg = fmt.Sprintf("Eventually aborted since scenario was cancelled")
			}
			break loop
		case <-waiterCtx.Done():
			errMsg = fmt.Sprintf("Eventually timed out after %s\n\n", w.timeout)
			break loop
		}
	}

	var sb strings.Builder
	sb.WriteString(errMsg)
	sb.WriteString(underlineStr("Property"))
	sb.WriteString(":\n")
	sb.WriteString(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())
	sb.WriteString("\n\n")
	sb.WriteString(underlineStr("State"))
	sb.WriteString(":\n")
	sb.WriteString(simpleSpew.Sdump(prop.owner))
	sb.WriteString("\n\n")
	sb.WriteString(underlineStr("Input"))
	sb.WriteString(":\n")
	sb.WriteString(fmt.Sprintf("%s", lastEvalCtx))
	s.Assertions().FailNow(sb.String())
}

func (s *Scenario) Assertions() *require.Assertions {
	return s.require
}

func (s *Scenario) T() *testing.T {
	return s.t
}

func (s *Scenario) genContext() *genContext {
	return s.genCtx
}
