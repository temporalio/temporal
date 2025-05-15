package testlogger

import (
	"cmp"
	"container/list"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

type Level string

const (
	Debug Level = "debug"
	Info  Level = "info"
	Warn  Level = "warn"
	// Panics, DPanics, and Fatal logs are considered errors
	Error Level = "error"
)

type Mode string

const (
	// FailOnAnyUnexpectedError mode will fail if any unexpected error is encountered,
	// like an allowlist. Use TestLogger.Expect to add an error to the allowlist.
	FailOnAnyUnexpectedError Mode = "fail-on-unexpected-errors"
	// FailOnExpectedErrorOnly mode will only fail if an expected error is encountered,
	// like a blocklist. Use TestLogger.Expect to add an error to the blocklist.
	FailOnExpectedErrorOnly = "fail-on-expected-errors"
)

// This is a subset of the testing.T interface that we use in this package that is
// also shared with *rapid.T.
type TestingT interface {
	zaptest.TestingT
	Helper()
	Name() string
	Logf(format string, args ...any)
	Log(args ...any)
	Fatalf(format string, args ...any)
	Fatal(args ...any)
}

// CleanupCapable is an interface that allows a test to register cleanup functions.
// This is /not/ implemented by *rapid.T but is by *testing.T
type CleanupCapableT interface {
	TestingT
	Cleanup(func())
}

func (l Level) String() string {
	return string(l)
}

// Expectations represent errors we expect to happen in tests.
// Their only purpose is to allow un-expecting (hah!) an error we've
// marked as expected
type Expectation struct {
	e          *list.Element
	testLogger *TestLogger
	lvl        Level
}

// Forget removes a previously registered expectation.
// A forgotten expectation will no longer be evaluated when errors are encountered.
func (e *Expectation) Forget() {
	e.testLogger.Forget(e)
}

type matcher struct {
	expectation *Expectation
	msg         *regexp.Regexp
	tags        map[string]*regexp.Regexp
}

func newMatcher(msg string, tags []tag.Tag, e *Expectation) matcher {
	var rgx *regexp.Regexp
	if msg != "" {
		rgx = regexp.MustCompile(msg)
	}
	m := matcher{
		expectation: e,
		msg:         rgx,
		tags:        map[string]*regexp.Regexp{},
	}
	for _, t := range tags {
		m.tags[t.Key()] = regexp.MustCompile(formatValue(t))
	}
	return m
}

func (m matcher) Matches(msg string, tags []tag.Tag) bool {
	// A nil regexp (meaning original string "") means we're only matching based on tags.
	if m.msg != nil && !m.msg.MatchString(msg) {
		return false
	}

	if len(m.tags) == 0 {
		return true
	}

	// We need to match all tags specified in the matcher,
	// not all tags on the message
	remainingMatches := len(m.tags)
	for _, t := range tags {
		pat, ok := m.tags[t.Key()]
		if !ok {
			continue
		}
		if pat.MatchString(formatValue(t)) {
			remainingMatches--
		}
	}
	return remainingMatches == 0
}

type sharedTestLoggerState struct {
	panicOnDPanic atomic.Bool
	panicOnError  atomic.Bool
	t             TestingT
	mu            struct {
		sync.RWMutex
		expectations map[Level]*list.List // Map[Level]List[matcher]
		closed       bool
		failed       bool
	}
	mode            Mode
	logExpectations bool
	level           zapcore.Level
}

// TestLogger is a log.Logger implementation that logs to the test's logger
// _but_ will fail the test if log levels _above_ Warn are present
type TestLogger struct {
	wrapped log.Logger
	state   *sharedTestLoggerState
	tags    []tag.Tag
}

type LoggerOption func(*TestLogger)

func DontPanicOnError(t *TestLogger) *TestLogger {
	t.state.panicOnError.Store(false)
	return t
}

func DontPanicOnDPanic(t *TestLogger) *TestLogger {
	t.state.panicOnDPanic.Store(false)
	return t
}

func LogExpectations(t *TestLogger) *TestLogger {
	t.state.logExpectations = true
	return t
}

// WithLogTags adds tags to the logs emitted by the underlying logger
func WithLogTags(tags ...tag.Tag) LoggerOption {
	return func(t *TestLogger) {
		t.tags = tags
	}
}

func WrapLogger(l log.Logger) LoggerOption {
	return func(t *TestLogger) {
		t.wrapped = l
	}
}

func LogLevel(level zapcore.Level) LoggerOption {
	return func(t *TestLogger) {
		t.state.level = level
	}
}

// SetLogLevel overrides the temporal test log level during this test.
func SetLogLevel(tt CleanupCapableT, level zapcore.Level) LoggerOption {
	return func(t *TestLogger) {
		oldLevel := os.Getenv("TEMPORAL_TEST_LOG_LEVEL")
		_ = os.Setenv("TEMPORAL_TEST_LOG_LEVEL", level.String())
		tt.Cleanup(func() {
			_ = os.Setenv("TEMPORAL_TEST_LOG_LEVEL", oldLevel)
		})

		t.state.level = level
	}
}

var _ log.Logger = (*TestLogger)(nil)

// NewTestLogger creates a new TestLogger that logs to the provided testing.T.
// Mode controls the behavior of the logger for when an expected or unexpected error is encountered.
func NewTestLogger(t TestingT, mode Mode, opts ...LoggerOption) *TestLogger {
	tl := &TestLogger{
		state: &sharedTestLoggerState{
			t:               t,
			logExpectations: false,
			level:           zapcore.DebugLevel,
			mode:            mode,
		},
	}
	tl.state.panicOnError.Store(true)
	tl.state.panicOnDPanic.Store(true)
	tl.state.mu.expectations = map[Level]*list.List{
		Debug: list.New(),
		Info:  list.New(),
		Warn:  list.New(),
		Error: list.New(),
	}
	for _, opt := range opts {
		opt(tl)
	}
	if tl.wrapped == nil {
		tl.wrapped = log.NewZapLogger(
			log.BuildZapLogger(
				log.Config{
					Level:  cmp.Or(os.Getenv(log.TestLogLevelEnvVar), tl.state.level.String()),
					Format: cmp.Or(os.Getenv(log.TestLogFormatEnvVar), "console"),
				}).
				WithOptions(
					zap.AddStacktrace(zap.ErrorLevel), // only include stack traces for logs with level error and above
				)).
			Skip(1)
	}

	// Only possible with a *testing.T until *rapid.T supports `Cleanup`
	if ct, ok := t.(CleanupCapableT); ok {
		// NOTE(tim): We don't care about anything logged after the test completes. Sure, this is racy,
		// but it reduces the likelihood that we see stupid errors due to testing.T.Logf race conditions...
		ct.Cleanup(tl.Close)
	}

	return tl
}

// ResetFailureStatus resets the failure state, returning the previous value.
// This is useful to verify that no unexpected errors were logged after the test
// completed (together with DontPanicOnError and/or DontPanicOnDPanic).
func (tl *TestLogger) ResetFailureStatus() bool {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	prevFailed := tl.state.mu.failed
	tl.state.mu.failed = false
	return prevFailed
}

// Expect instructs the logger to expect certain errors, as specified by the msg and tag arguments.
// Depending on the Mode of the test logger, the expectation either acts as an entry in a
// blocklist (FailOnExpectedErrorOnly) or an allowlist (FailOnAnyUnexpectedError).
func (tl *TestLogger) Expect(level Level, msg string, tags ...tag.Tag) *Expectation {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	if tl.state.logExpectations {
		tl.wrapped.Info(fmt.Sprintf("(%p) TestLogger::Expecting: '%s'\n", tl, msg))
	}
	e := &Expectation{
		testLogger: tl,
		lvl:        level,
	}
	m := newMatcher(msg, tags, e)
	e.e = tl.state.mu.expectations[level].PushBack(m)
	return e
}

// Forget removes a previously registered expectation.
// A forgotten expectation will no longer be evaluated when errors are encountered.
func (tl *TestLogger) Forget(e *Expectation) {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	tl.state.mu.expectations[e.lvl].Remove(e.e)
}

func (tl *TestLogger) shouldFailTest(level Level, msg string, tags []tag.Tag) bool {
	expectations := tl.state.mu.expectations[level]
	for e := expectations.Front(); e != nil; e = e.Next() {
		m, ok := e.Value.(matcher)
		if !ok {
			tl.state.t.Fatalf("Bug in TestLogger: invalid %T value in matcher list", e.Value)
		}
		if m.Matches(msg, tags) {
			return tl.state.mode == FailOnExpectedErrorOnly
		}
	}
	return tl.state.mode == FailOnAnyUnexpectedError
}

// PanicOnError overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
func (tl *TestLogger) PanicOnError(v bool) bool {
	return tl.state.panicOnError.Swap(v)
}

func (tl *TestLogger) mergeTags(tags []tag.Tag) []tag.Tag {
	if tl.tags == nil {
		return tags
	}
	tagMap := make(map[string]tag.Tag, len(tl.tags)+len(tags))
	for _, t := range tl.tags {
		tagMap[t.Key()] = t
	}
	for _, t := range tags {
		tagMap[t.Key()] = t
	}
	newTags := make([]tag.Tag, 0, len(tagMap))
	for _, t := range tagMap {
		newTags = append(newTags, t)
	}
	return newTags
}

// DPanic implements log.Logger.
func (tl *TestLogger) DPanic(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeTags(tags)
	// note, actual panic'ing in wrapped is turned off so we can control.
	tl.wrapped.DPanic(msg, tags...)
	if tl.state.panicOnDPanic.Load() && tl.shouldFailTest(Error, msg, tags) {
		tl.state.t.Helper()
		panic(failureMessage("DPanic", msg, tags))
	}
}

// Debug implements log.Logger.
func (tl *TestLogger) Debug(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.wrapped.Debug(msg, tl.mergeTags(tags)...)
}

// Error implements log.Logger.
func (tl *TestLogger) Error(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	if tl.state.mu.closed {
		tl.state.mu.RUnlock()
		return
	}
	tags = tl.mergeTags(tags)
	if !tl.shouldFailTest(Error, msg, tags) {
		tl.wrapped.Error(msg, tags...)
		tl.state.mu.RUnlock()
		return
	}
	tl.state.mu.RUnlock()

	if tl.state.panicOnError.Load() {
		tl.state.t.Helper()
		tl.wrapped.Error(msg, tags...)
		panic(failureMessage("Error", msg, tags))
	}

	// Labeling the error as unexpected; so it can easily be identified later.
	tl.wrapped.Error(errorMessage("Error", msg), tags...)

	// Not panic'ing, so marking the test as failed.
	tl.state.mu.Lock()
	tl.state.mu.failed = true
	tl.state.mu.Unlock()
}

// Fatal implements log.Logger.
func (tl *TestLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.state.t.Helper()
	tl.state.t.Fatal(failureMessage("Fatal", msg, tl.mergeTags(tags)))
}

// Info implements log.Logger.
func (tl *TestLogger) Info(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.wrapped.Info(msg, tl.mergeTags(tags)...)
}

// Panic implements log.Logger.
func (tl *TestLogger) Panic(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.state.t.Helper()
	tl.state.t.Fatal(failureMessage("Panic", msg, tl.mergeTags(tags)))
}

// Warn implements log.Logger.
func (tl *TestLogger) Warn(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.wrapped.Warn(msg, tl.mergeTags(tags)...)
}

// WithTags gives you a new logger, copying the tags of the source, appending the provided new Tags
func (tl *TestLogger) WithTags(tags ...tag.Tag) *TestLogger {
	allTags := make([]tag.Tag, len(tl.tags)+len(tags))
	copy(allTags, tl.tags)
	copy(allTags[len(tl.tags):], tags)
	newtl := &TestLogger{
		wrapped: tl.wrapped,
		state:   tl.state,
		tags:    allTags,
	}
	return newtl
}

// Close disallows any further logging, preventing the test framework from complaining about
// logging post-test.
func (tl *TestLogger) Close() {
	// Taking the write lock ensures all in-progress log calls complete before we close.
	// This prevents a race condition after the test has completed.
	tl.state.mu.Lock()
	tl.state.mu.closed = true
	tl.state.mu.Unlock()
}

func (tl *TestLogger) T() TestingT {
	return tl.state.t
}

var _ log.WithLogger = (*TestLogger)(nil)

// With implements log.WithLogger
func (tl *TestLogger) With(tags ...tag.Tag) log.Logger {
	return &TestLogger{
		wrapped: tl.wrapped,
		state:   tl.state,
		tags:    tl.mergeTags(tags),
	}
}

// Format the log.Logger tags and such into a useful message
func failureMessage(level string, msg string, tags []tag.Tag) string {
	var b strings.Builder
	b.WriteString("FAIL: ")
	b.WriteString(errorMessage(level, msg))
	for _, t := range tags {
		b.WriteString(t.Key())
		b.WriteString("=")
		b.WriteString(formatValue(t))
		b.WriteString(" ")
	}
	return b.String()
}

func errorMessage(level string, msg string) string {
	var b strings.Builder
	b.WriteString("Unexpected ")
	b.WriteString(level)
	b.WriteString(" log encountered: ")
	b.WriteString(msg)
	return b.String()
}

func formatValue(t tag.Tag) string {
	switch v := t.Value().(type) {
	case fmt.Stringer:
		return v.String()
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
