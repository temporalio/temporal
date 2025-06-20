package testlogger

import (
	"cmp"
	"container/list"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

//go:generate stringer -type Level -trimprefix Level -output testlogger.gen.go
type Level int

const (
	Debug Level = iota
	Info
	Warn
	Error
	Fatal
	DPanic
	Panic
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
	failOnDPanic atomic.Bool
	failOnError  atomic.Bool
	failOnFatal  atomic.Bool
	t            TestingT
	mu           struct {
		sync.RWMutex
		expectations map[Level]*list.List // Map[Level]List[matcher]
		closed       bool
	}
	mode            Mode
	logExpectations bool
	logCaller       bool
	level           zapcore.Level
}

// TestLogger is a log.Logger implementation that logs to the test's logger
// _but_ will fail the test if log levels _above_ Warn are present
type TestLogger struct {
	wrapped log.Logger
	state   *sharedTestLoggerState
	tags    []tag.Tag
	// If false the caller will not be logged by Zap. This is used when we process the
	// logs from a subprocesses' STDOUT as we don't care about where in the main process
	// the call came from, just where it was logged by the child.
}

type LoggerOption func(*TestLogger)

func DontFailOnError(t *TestLogger) *TestLogger {
	t.state.failOnError.Store(false)
	return t
}

func DontFailOnDPanic(t *TestLogger) *TestLogger {
	t.state.failOnDPanic.Store(false)
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

// WithoutCaller stops the TestLogger from logging the caller.
// This is primarily intended for use when parsing log.Logger-generated logs in files
// or coming from subprocesses.
func WithoutCaller() LoggerOption {
	return func(t *TestLogger) {
		t.state.logCaller = false
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
			logCaller:       true,
			mode:            mode,
		},
	}
	tl.state.mu.expectations = make(map[Level]*list.List)
	tl.state.failOnError.Store(true)
	tl.state.failOnDPanic.Store(true)
	tl.state.failOnFatal.Store(true)
	for _, opt := range opts {
		opt(tl)
	}
	if tl.wrapped == nil {
		writer := zaptest.NewTestingWriter(t)
		var enc zapcore.Encoder
		format := cmp.Or(os.Getenv(log.TestLogFormatEnvVar), "console")
		switch strings.ToLower(format) {
		case "console":
			enc = zapcore.NewConsoleEncoder(log.DefaultZapEncoderConfig)
		case "json":
			enc = zapcore.NewJSONEncoder(log.DefaultZapEncoderConfig)
		default:
			t.Fatalf("unknown log encoding %q", format)
		}
		level := tl.state.level
		if levelV := os.Getenv(log.TestLogLevelEnvVar); levelV != "" {
			level = log.ParseZapLevel(levelV)
		}
		core := zapcore.NewCore(enc, writer, level)
		zapOptions := []zap.Option{
			// Send zap errors to the same writer and mark the test as failed if
			// that happens.
			zap.ErrorOutput(writer.WithMarkFailed(true)),
			zap.AddStacktrace(zap.ErrorLevel), // only include stack traces for logs with level error and above
			zap.WithCaller(tl.state.logCaller),
		}

		// Skip(1) skips the TestLogger itself
		tl.wrapped = log.NewZapLogger(zap.New(core, zapOptions...)).Skip(1)
	}

	// Only possible with a *testing.T until *rapid.T supports `Cleanup`
	if ct, ok := t.(CleanupCapableT); ok {
		// NOTE(tim): We don't care about anything logged after the test completes. Sure, this is racy,
		// but it reduces the likelihood that we see stupid errors due to testing.T.Logf race conditions...
		ct.Cleanup(tl.Close)
	}

	return tl
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
	expectations, ok := tl.state.mu.expectations[level]
	if !ok {
		expectations = list.New()
		tl.state.mu.expectations[level] = expectations
	}
	e.e = expectations.PushBack(m)
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
	// Only check expectations if they've been registered for this level.
	if expectations, found := tl.state.mu.expectations[level]; found {
		for e := expectations.Front(); e != nil; e = e.Next() {
			m, ok := e.Value.(matcher)
			if !ok {
				tl.state.t.Fatalf("Bug in TestLogger: invalid %T value in matcher list", e.Value)
			}
			if m.Matches(msg, tags) {
				return tl.state.mode == FailOnExpectedErrorOnly
			}
		}
	}
	if level < Error {
		// We don't care about a lack of debug/info/warn expectations, only Error and above
		return false
	}
	return tl.state.mode == FailOnAnyUnexpectedError
}

// FailOnDPanic overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
func (tl *TestLogger) FailOnDPanic(b bool) bool {
	return tl.state.failOnDPanic.Swap(b)
}

// FailOnError overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
func (tl *TestLogger) FailOnError(b bool) bool {
	return tl.state.failOnError.Swap(b)
}

// FailOnFatal overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
// Note that Fatal-level logs still panic
func (tl *TestLogger) FailOnFatal(b bool) bool {
	return tl.state.failOnFatal.Swap(b)
}

func (tl *TestLogger) mergeWithLoggerTags(tags []tag.Tag) []tag.Tag {
	if len(tl.tags) == 0 {
		return tags
	}
	tagMap := make(map[string]tag.Tag, len(tl.tags)+len(tags))
	// Iterate over the logger's tags first so that explicitly specified tags override them
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
	slices.SortStableFunc(newTags, func(a, b tag.Tag) int {
		return cmp.Compare(a.Key(), b.Key())
	})
	return newTags
}

// DPanic implements log.Logger.
func (tl *TestLogger) DPanic(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	// note, actual panic'ing in wrapped is turned off so we can control.
	tl.wrapped.DPanic(msg, tags...)
	if tl.state.failOnDPanic.Load() && tl.shouldFailTest(DPanic, msg, tags) {
		tl.state.t.Helper()
		tl.failTest(DPanic, msg, tags...)
	}
}

// Debug implements log.Logger.
func (tl *TestLogger) Debug(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.wrapped.Debug(msg, tl.mergeWithLoggerTags(tags)...)
}

// Error implements log.Logger.
func (tl *TestLogger) Error(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	if tl.state.mu.closed {
		tl.state.mu.RUnlock()
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	if !tl.shouldFailTest(Error, msg, tags) {
		tl.wrapped.Error(msg, tags...)
		tl.state.mu.RUnlock()
		return
	}
	tl.state.mu.RUnlock()

	if tl.state.failOnError.Load() {
		tl.state.t.Helper()
		tl.wrapped.Error(msg, tags...)
		tl.failTest(Error, msg, tags...)
	}

	// Labeling the error as unexpected; so it can easily be identified later.
	tl.wrapped.Error(errorMessage(Error, msg), tags...)
}

// Fatal implements log.Logger.
func (tl *TestLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.state.t.Helper()
	if tl.state.failOnFatal.Load() && tl.shouldFailTest(Fatal, msg, tags) {
		tl.failTest(Fatal, msg, tags...)
	}
	// Panic to emulate a fatal in a way we can catch.
	// Use of this requires that the testing code catches the ensuing panic
	// NOTE: This will not work if the code under test that catches panics, but we've no other option.
	if tl.state.failOnFatal.Load() {
		//nolint:forbidigo
		tl.wrapped.Panic(fmt.Sprintf("FATAL: %s", msg), tags...)
	}
	tl.wrapped.Error(fmt.Sprintf("FATAL: %s", msg), tags...)
}

// Info implements log.Logger.
func (tl *TestLogger) Info(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.wrapped.Info(msg, tl.mergeWithLoggerTags(tags)...)
}

// Panic implements log.Logger.
func (tl *TestLogger) Panic(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.state.t.Helper()
	// Forcibly fail the test when required as otherwise panics can be caught.
	if tl.shouldFailTest(Panic, msg, tags) {
		tl.state.t.Helper()
		tl.failTest(Panic, msg, tags...)
	}
	tl.wrapped.Panic(message(Panic, msg, tags))
}

// Warn implements log.Logger.
func (tl *TestLogger) Warn(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tl.wrapped.Warn(msg, tl.mergeWithLoggerTags(tags)...)
}

// failTest fails the test while dumping the stack, allowing us to know where in the code
// the failure arose.
func (tl *TestLogger) failTest(level Level, msg string, tags ...tag.Tag) {
	tl.state.t.Helper()
	skip := 2                   // skip the invocation of failTest and the log.Logger function that called it
	pcs := make([]uintptr, 128) // 128 is likely larger that we'll ever need.
	frameCount := runtime.Callers(skip, pcs)

	// runtime.Callers truncates the recorded stacktrace to fit the provided slice.
	// Just keep doubling in size until we have enough space.
	for frameCount == len(pcs) {
		pcs = make([]uintptr, len(pcs)*2)
		frameCount = runtime.Callers(skip+2, pcs)
	}

	stackFrames := runtime.CallersFrames(pcs)
	var stackTrace strings.Builder
	for frame, more := stackFrames.Next(); more; frame, more = stackFrames.Next() {
		fmt.Fprintf(&stackTrace, "%s:%d %s\n", frame.File, frame.Line, frame.Function)
	}

	tl.state.t.Fatalf("%s\n%s", failureMessage(level, msg, tags), stackTrace.String())
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
		tags:    tl.mergeWithLoggerTags(tags),
	}
}

// Format the log.Logger tags and such into a useful message
func failureMessage(level Level, msg string, tags []tag.Tag) string {
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

func errorMessage(level Level, msg string) string {
	var b strings.Builder
	b.WriteString("Unexpected ")
	b.WriteString(level.String())
	b.WriteString(" log encountered: ")
	b.WriteString(msg)
	return b.String()
}

func message(level Level, msg string, tags []tag.Tag) string {
	var b strings.Builder
	b.WriteString(level.String())
	b.WriteRune(':')
	b.WriteString(msg)
	b.WriteString("\" ")
	for _, t := range tags {
		b.WriteString(t.Key())
		b.WriteString("=")
		b.WriteString(formatValue(t))
		b.WriteString(" ")
	}
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
