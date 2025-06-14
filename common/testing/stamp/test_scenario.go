package stamp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tDebug "go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
)

var (
	_                      testEnv = (*Scenario)(nil)
	defaultActionTimeout           = 5 * time.Second
	defaultScenarioTimeout         = 10 * time.Second
	defaultWaiterTimeout           = 3 * time.Second
	defaultWaiterPolling           = 100 * time.Millisecond
)

type (
	Scenario struct {
		t           *testing.T
		logger      log.Logger
		loggerFn    func(testing.TB) log.Logger
		require     *require.Assertions
		genCtx      *genContext
		timeout     time.Duration
		ctx         context.Context
		ctxCancelFn context.CancelFunc
		happyCase   bool
		genCache    map[string]any
		opts        []ScenarioOption
	}
	waiter struct {
		timeout time.Duration
		backoff time.Duration
		onRetry func()
	}
	ScenarioOption interface {
		applyScenario(*Scenario)
	}
	WaiterOption interface {
		applyWaiter(*waiter)
	}
	LabeledFunction struct {
		Label string
		Fn    func()
	}
	option[T any] struct {
		Label string
		Value T
	}
)

// WithTimeout is an option to set a timeout.
func WithTimeout(timeout time.Duration) timeoutOption {
	return timeoutOption{duration: timeout}
}

type timeoutOption struct {
	duration time.Duration
}

func (o timeoutOption) applyScenario(s *Scenario) {
	s.timeout = o.duration
}

func (o timeoutOption) applyWaiter(w *waiter) {
	w.timeout = o.duration
}

func OnRetry(onRetry func()) WaiterOption {
	return onRetryOption{onRetry: onRetry}
}

type onRetryOption struct {
	onRetry func()
}

func (o onRetryOption) applyWaiter(w *waiter) {
	w.onRetry = o.onRetry
}

// WithBackoff is an option to set a backoff interval for a waiter.
func WithBackoff(interval time.Duration) WaiterOption {
	return backoffOption{interval: interval}
}

type backoffOption struct {
	interval time.Duration
}

func (o backoffOption) applyWaiter(w *waiter) {
	w.backoff = o.interval
}

// HappyCase is an option to mark the scenario as a happy case.
// A happy case scenario is run before all other scenarios to ensure
// that the basic functionality is working as expected.
// TODO: implement
func HappyCase() ScenarioOption {
	return happyCaseOption{}
}

type happyCaseOption struct{}

func (o happyCaseOption) applyScenario(s *Scenario) {
	s.happyCase = true
}

type allowRandomOption struct{}

func (o allowRandomOption) applyScenario(s *Scenario) {
	s.genCtx.allowRandom = true
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
	loggerFn func(testing.TB) log.Logger,
	opts ...ScenarioOption,
) *Scenario {
	t.Helper()

	s := &Scenario{
		t:        t,
		logger:   loggerFn(t),
		loggerFn: loggerFn,
		genCtx:   newGenContext(newSeed(t.Name())),
		require:  require.New(t),
		timeout:  defaultScenarioTimeout,
		genCache: make(map[string]any),
	}

	s.opts = opts
	for _, opt := range opts {
		opt.applyScenario(s)
	}

	s.ctx, s.ctxCancelFn = context.WithTimeout(context.Background(), s.timeout)
	return s
}

func (s *Scenario) Logger() log.Logger {
	return s.logger
}

func (s *Scenario) Context(timeout time.Duration) context.Context {
	c, _ := context.WithTimeout(s.ctx, timeout)
	return c
}

func (s *Scenario) Ensure(p propAccessor) {
	s.t.Helper()
	if p.getVal() != true {
		s.require.FailNow("expected property '" + p.getName() + "' to be true, but got false")
	}
}

// TODO: make this a top-level function that works for properties and futures
func (s *Scenario) Await(p propAccessor, opts ...WaiterOption) {
	s.t.Helper()

	w := &waiter{
		timeout: defaultWaiterTimeout,
		backoff: defaultWaiterPolling,
	}
	for _, opt := range opts {
		opt.applyWaiter(w)
	}

	waiterCtx, cancelFn := context.WithTimeout(context.Background(), w.timeout*tDebug.TimeoutMultiplier)
	defer cancelFn()

	ticker := time.NewTicker(w.backoff)
	defer ticker.Stop()

	var errMsg string
loop:
	for {
		select {
		case <-ticker.C:
			if w.onRetry != nil {
				w.onRetry()
			}
			if p.getVal() == true {
				return
			}
		case <-s.ctx.Done():
			if ctxErr := s.ctx.Err(); errors.Is(ctxErr, context.DeadlineExceeded) {
				errMsg = fmt.Sprintf("Await aborted since scenario timed out after %s\n\n", s.timeout)
			} else {
				errMsg = fmt.Sprintf("Await aborted since scenario was cancelled")
			}
			break loop
		case <-waiterCtx.Done():
			errMsg = fmt.Sprintf("Await timed out after %s\n\n", w.timeout)
			break loop
		}
	}

	var sb strings.Builder
	sb.WriteString(errMsg)
	sb.WriteString(underlineStr("Property"))
	sb.WriteString(":\n")
	sb.WriteString(p.getName())
	sb.WriteString("\n\n")
	sb.WriteString(underlineStr("State"))
	sb.WriteString(":\n")
	sb.WriteString(simpleSpew.Sdump(p.getOwner()))
	sb.WriteString("\n\n")
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

// TODO: check there is no duplicate scenario run
func (s *Scenario) Run(testFn func(*Scenario)) {
	// ==== exploration run to collect all choice generators

	type genInfo struct {
		choices int
		index   int
	}
	genIdx := map[string]genInfo{}
	s.genCtx.pickChoiceFn = func(id string, choices int) int {
		s.t.Logf("found %s", id)
		if choices <= 0 {
			panic("generator has no variants")
		}
		genIdx[id] = genInfo{
			choices: choices,
			index:   len(genIdx),
		}
		return 0 // always pick the first choice on the first run
	}

	testFn(s)
	if s.t.Failed() {
		return
	}
	if len(genIdx) == 0 {
		return // no generators found, nothing to run
	}

	// ==== run all other scenarios

	// calculate the total number of scenarios
	total := 1
	for _, info := range genIdx {
		total *= info.choices
	}
	s.t.Logf("found %d scenarios across %d generators", total, len(genIdx))

	for i := 1; i < total; i++ { // start from 1 to avoid running the first scenario again
		s.t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Parallel() // enforce parallel execution
			nestedScenario := newScenario(t, s.loggerFn, s.opts...)

			nestedScenario.genCtx.pickChoiceFn = func(id string, choices int) int {
				info, exists := genIdx[id]
				if !exists {
					panic(fmt.Sprintf("found unknown generator after exploration: %s", id))
				}

				divisor := 1
				for otherId, otherInfo := range genIdx {
					if otherInfo.index > info.index {
						if otherInfo.choices <= 0 {
							panic(fmt.Sprintf("generator %s has invalid variant count: %d", otherId, otherInfo.choices))
						}
						divisor *= otherInfo.choices
					}
				}

				pick := (i / divisor) % info.choices
				return pick
			}

			testFn(nestedScenario)
		})
	}
}

func (s *Scenario) Skip(args ...any) {
	s.t.Skip(args...)
}

func (s *Scenario) VerifyOnce() {
	// TODO
}

// TODO: panic if same name is used twice
// TODO: `any` and `all` combinators like `sync.WaitGroup`
func (s *Scenario) Maybe(name string) bool {
	s.t.Helper()

	name = fmt.Sprintf("Maybe(%s)", name)
	gen := GenEnum(name, true, false)
	choice := resolveChoice(s, gen, name)
	return choice
}

func (s *Scenario) Switch(name string, choices ...LabeledFunction) {
	s.t.Helper()

	name = fmt.Sprintf("Switch(%s)", name)
	gen := GenEnum(name, choices...)
	choice := resolveChoice(s, gen, name)
	choice.Fn()
}

func Select[T any](s *Scenario, name string, options ...option[T]) T {
	s.t.Helper()

	name = fmt.Sprintf("Switch(%s)", name)
	gen := GenEnum(name, options...)
	return resolveChoice(s, gen, name).Value
}

func Option[T any](label string, val T) option[T] {
	return option[T]{
		Label: label,
		Value: val,
	}
}

func resolveChoice[T any](s *Scenario, gen Gen[T], name string) T {
	if cached, ok := s.genCache[name]; ok {
		return cached.(T)
	}
	res := gen.Next(s)
	s.genCache[name] = res
	return res
}
