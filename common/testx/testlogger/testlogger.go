package testlogger

import (
	"cmp"
	"container/list"
	"fmt"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

// Logger is implemented by *zap.Logger.
type Logger interface {
	Debug(string, ...zap.Field)
	Info(string, ...zap.Field)
	Warn(string, ...zap.Field)
	Error(string, ...zap.Field)
	DPanic(string, ...zap.Field)
	Panic(string, ...zap.Field)
	Fatal(string, ...zap.Field)
}

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

// TestingT is a subset of the testing.T interface that we use in this package that is
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

// CleanupCapableT is an interface that allows a test to register cleanup functions.
// This is /not/ implemented by *rapid.T but is by *testing.T
type CleanupCapableT interface {
	TestingT
	Cleanup(func())
}

// Expectation represents a log call we expect to happen in tests.
// For error-level logs, expectations also influence whether the log fails the
// test based on the logger mode.
type Expectation struct {
	e          *list.Element
	testLogger *TestLogger
	lvl        Level
	matches    atomic.Int64
}

// Forget removes a previously registered expectation.
// A forgotten expectation will no longer be evaluated when logs are encountered.
func (e *Expectation) Forget() {
	e.testLogger.Forget(e)
}

func (e *Expectation) Matched() bool {
	return e.MatchCount() > 0
}

func (e *Expectation) MatchCount() int64 {
	return e.matches.Load()
}

type matcher struct {
	expectation *Expectation
	msg         *regexp.Regexp
	fields      map[string]*regexp.Regexp
}

func newMatcher(msg string, fields []zap.Field, e *Expectation) matcher {
	var rgx *regexp.Regexp
	if msg != "" {
		rgx = regexp.MustCompile(msg)
	}
	m := matcher{
		expectation: e,
		msg:         rgx,
		fields:      map[string]*regexp.Regexp{},
	}
	for _, t := range fields {
		m.fields[t.Key] = regexp.MustCompile(formatValue(t))
	}
	return m
}

func (m matcher) Matches(msg string, fields []zap.Field) bool {
	// A nil regexp (meaning original string "") means we're only matching based on fields.
	if m.msg != nil && !m.msg.MatchString(msg) {
		return false
	}

	if len(m.fields) == 0 {
		return true
	}

	// We need to match all fields specified in the matcher,
	// not all fields on the message
	remainingMatches := len(m.fields)
	for _, t := range fields {
		pat, ok := m.fields[t.Key]
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
		captures     map[*Capture]struct{}
		closed       bool
	}
	mode            Mode
	logExpectations bool
	logCaller       bool
	level           zapcore.Level
	failure         atomic.Pointer[Failure]
}

// Failure describes a log call that shouldFailTest considered a test-failing log.
type Failure struct {
	Level  Level
	Msg    string
	Fields []zap.Field
	Stack  string
}

// TestLogger is a Logger implementation that logs to the test's logger
// _but_ will fail the test if log levels _above_ Warn are present
type TestLogger struct {
	wrapped Logger
	state   *sharedTestLoggerState
	fields  []zap.Field
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

// WithFields adds fields to the logs emitted by the underlying logger
func WithFields(fields ...zap.Field) LoggerOption {
	return func(t *TestLogger) {
		t.fields = fields
	}
}

func WrapLogger(l Logger) LoggerOption {
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
// This is primarily intended for use when parsing Logger-generated logs in files
// or coming from subprocesses.
func WithoutCaller() LoggerOption {
	return func(t *TestLogger) {
		t.state.logCaller = false
	}
}

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
	tl.state.mu.captures = make(map[*Capture]struct{})
	tl.state.failOnError.Store(true)
	tl.state.failOnDPanic.Store(true)
	tl.state.failOnFatal.Store(true)
	for _, opt := range opts {
		opt(tl)
	}
	if tl.wrapped == nil {
		writer := zaptest.NewTestingWriter(t)

		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), writer, tl.state.level)
		tl.wrapped = zap.New(core,
			zap.ErrorOutput(writer.WithMarkFailed(true)),
			zap.AddStacktrace(zap.ErrorLevel), // only include stack traces for logs with level error and above
			zap.WithCaller(tl.state.logCaller),
			zap.AddCallerSkip(1),
		)
	}

	// Only possible with a *testing.T until *rapid.T supports `Cleanup`
	if ct, ok := t.(CleanupCapableT); ok {
		// NOTE(tim): We don't care about anything logged after the test completes. Sure, this is racy,
		// but it reduces the likelihood that we see stupid errors due to testing.T.Logf race conditions...
		ct.Cleanup(tl.Close)
	}

	return tl
}

// Expect instructs the logger to track matching log calls, as specified by the
// level, msg, and field arguments. For error-level logs, depending on the Mode of
// the test logger, the expectation either acts as an entry in a blocklist
// (FailOnExpectedErrorOnly) or an allowlist (FailOnAnyUnexpectedError).
func (tl *TestLogger) Expect(level Level, msg string, fields ...zap.Field) *Expectation {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	if tl.state.logExpectations {
		tl.wrapped.Info(fmt.Sprintf("(%p) TestLogger::Expecting: '%s'\n", tl, msg))
	}
	e := &Expectation{
		testLogger: tl,
		lvl:        level,
	}
	m := newMatcher(msg, fields, e)
	expectations, ok := tl.state.mu.expectations[level]
	if !ok {
		expectations = list.New()
		tl.state.mu.expectations[level] = expectations
	}
	e.e = expectations.PushBack(m)
	return e
}

// Forget removes a previously registered expectation.
// A forgotten expectation will no longer be evaluated when logs are encountered.
func (tl *TestLogger) Forget(e *Expectation) {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	tl.state.mu.expectations[e.lvl].Remove(e.e)
}

// StartCapture starts recording log calls that contain any of the provided exact fields.
// It records all calls when no fields are provided.
func (tl *TestLogger) StartCapture(anyFields ...zap.Field) *Capture {
	capture := newCapture(anyFields)
	tl.state.mu.Lock()
	tl.state.mu.captures[capture] = struct{}{}
	tl.state.mu.Unlock()
	return capture
}

// StopCapture stops recording log calls for capture.
func (tl *TestLogger) StopCapture(capture *Capture) {
	tl.state.mu.Lock()
	delete(tl.state.mu.captures, capture)
	tl.state.mu.Unlock()
}

func (tl *TestLogger) shouldFailTest(level Level, msg string, fields []zap.Field) bool {
	// Only check expectations if they've been registered for this level.
	if expectations, found := tl.state.mu.expectations[level]; found {
		for e := expectations.Front(); e != nil; e = e.Next() {
			m, ok := e.Value.(matcher)
			if !ok {
				tl.state.t.Fatalf("Bug in TestLogger: invalid %T value in matcher list", e.Value)
			}
			if m.Matches(msg, fields) {
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

// recordExpectationMatches increments the match counter of every expectation
// registered at the given level whose matcher matches this log call. It is purely
// observational: it never affects whether the test fails (that remains the sole
// job of shouldFailTest), so it is safe to call for any log at any level.
func (tl *TestLogger) recordExpectationMatches(level Level, msg string, fields []zap.Field) {
	expectations, found := tl.state.mu.expectations[level]
	if !found {
		return
	}
	for e := expectations.Front(); e != nil; e = e.Next() {
		m, ok := e.Value.(matcher)
		if !ok {
			tl.state.t.Fatalf("Bug in TestLogger: invalid %T value in matcher list", e.Value)
		}
		if m.Matches(msg, fields) {
			m.expectation.matches.Add(1)
		}
	}
}

func (tl *TestLogger) recordCaptures(level Level, msg string, fields []zap.Field) {
	if len(tl.state.mu.captures) == 0 {
		return
	}
	record := CapturedLog{
		Level:   level,
		Message: msg,
		Fields:  slices.Clone(fields),
	}
	for capture := range tl.state.mu.captures {
		capture.record(record)
	}
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

// Failure returns the first Failure that shouldFailTest considered a test-failing
// log, or nil if no such log has been observed. Sticky: first failure wins.
func (tl *TestLogger) Failure() *Failure {
	return tl.state.failure.Load()
}

// recordFailure stores the first observed failure (first-failure-wins via CAS).
// Fields are copied so the recorded value is independent of the caller's slice.
func (tl *TestLogger) recordFailure(level Level, msg string, fields []zap.Field) {
	if tl.state.failure.Load() != nil {
		// fast-path: already recorded; avoid the copy and the stack capture.
		return
	}
	fieldsCopy := make([]zap.Field, len(fields))
	copy(fieldsCopy, fields)
	f := &Failure{
		Level:  level,
		Msg:    msg,
		Fields: fieldsCopy,
		Stack:  captureStack(3), // skip captureStack, recordFailure, and the Logger method that called it
	}
	tl.state.failure.CompareAndSwap(nil, f)
}

func (tl *TestLogger) mergeWithLoggerFields(fields []zap.Field) []zap.Field {
	if len(tl.fields) == 0 {
		return fields
	}
	tagMap := make(map[string]zap.Field, len(tl.fields)+len(fields))
	// Iterate over the logger's fields first so that explicitly specified fields override them
	for _, t := range tl.fields {
		tagMap[t.Key] = t
	}
	for _, t := range fields {
		tagMap[t.Key] = t
	}
	newFields := make([]zap.Field, 0, len(tagMap))
	for _, t := range tagMap {
		newFields = append(newFields, t)
	}
	slices.SortStableFunc(newFields, func(a, b zap.Field) int {
		return cmp.Compare(a.Key, b.Key)
	})
	return newFields
}

// DPanic implements Logger.
func (tl *TestLogger) DPanic(msg string, fields ...zap.Field) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	fields = tl.mergeWithLoggerFields(fields)
	tl.recordExpectationMatches(DPanic, msg, fields)
	tl.recordCaptures(DPanic, msg, fields)
	// note, actual panic'ing in wrapped is turned off so we can control.
	tl.wrapped.DPanic(msg, fields...)
	if tl.state.failOnDPanic.Load() && tl.shouldFailTest(DPanic, msg, fields) {
		tl.state.t.Helper()
		tl.failTest(DPanic, msg, fields...)
	}
}

// Debug implements Logger.
func (tl *TestLogger) Debug(msg string, fields ...zap.Field) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	fields = tl.mergeWithLoggerFields(fields)
	tl.recordExpectationMatches(Debug, msg, fields)
	tl.recordCaptures(Debug, msg, fields)
	tl.wrapped.Debug(msg, fields...)
}

// Error implements Logger.
func (tl *TestLogger) Error(msg string, fields ...zap.Field) {
	tl.state.mu.RLock()
	if tl.state.mu.closed {
		tl.state.mu.RUnlock()
		return
	}
	fields = tl.mergeWithLoggerFields(fields)
	tl.recordExpectationMatches(Error, msg, fields)
	tl.recordCaptures(Error, msg, fields)
	if !tl.shouldFailTest(Error, msg, fields) {
		tl.wrapped.Error(msg, fields...)
		tl.state.mu.RUnlock()
		return
	}
	tl.state.mu.RUnlock()

	if tl.state.failOnError.Load() {
		tl.state.t.Helper()
		tl.wrapped.Error(msg, fields...)
		tl.failTest(Error, msg, fields...)
	}

	// Labeling the error as unexpected; so it can easily be identified later.
	tl.wrapped.Error(errorMessage(Error, msg), fields...)
}

// Fatal implements Logger.
func (tl *TestLogger) Fatal(msg string, fields ...zap.Field) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	fields = tl.mergeWithLoggerFields(fields)
	tl.recordExpectationMatches(Fatal, msg, fields)
	tl.recordCaptures(Fatal, msg, fields)
	tl.state.t.Helper()
	if tl.state.failOnFatal.Load() && tl.shouldFailTest(Fatal, msg, fields) {
		tl.failTest(Fatal, msg, fields...)
	}
	// Panic to emulate a fatal in a way we can catch.
	// Use of this requires that the testing code catches the ensuing panic
	// NOTE: This will not work if the code under test that catches panics, but we've no other option.
	if tl.state.failOnFatal.Load() {
		//nolint:forbidigo
		tl.wrapped.Panic(fmt.Sprintf("FATAL: %s", msg), fields...)
	}
	tl.wrapped.Error(fmt.Sprintf("FATAL: %s", msg), fields...)
}

// Info implements Logger.
func (tl *TestLogger) Info(msg string, fields ...zap.Field) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	fields = tl.mergeWithLoggerFields(fields)
	tl.recordExpectationMatches(Info, msg, fields)
	tl.recordCaptures(Info, msg, fields)
	tl.wrapped.Info(msg, fields...)
}

// Panic implements Logger.
func (tl *TestLogger) Panic(msg string, fields ...zap.Field) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	fields = tl.mergeWithLoggerFields(fields)
	tl.recordExpectationMatches(Panic, msg, fields)
	tl.recordCaptures(Panic, msg, fields)
	tl.state.t.Helper()
	// Forcibly fail the test when required as otherwise panics can be caught.
	if tl.shouldFailTest(Panic, msg, fields) {
		tl.state.t.Helper()
		tl.failTest(Panic, msg, fields...)
	}
	tl.wrapped.Panic(message(Panic, msg, fields))
}

// Warn implements Logger.
func (tl *TestLogger) Warn(msg string, fields ...zap.Field) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	fields = tl.mergeWithLoggerFields(fields)
	tl.recordExpectationMatches(Warn, msg, fields)
	tl.recordCaptures(Warn, msg, fields)
	tl.wrapped.Warn(msg, fields...)
}

// failTest fails the test while dumping the stack, allowing us to know where in the code
// the failure arose.
func (tl *TestLogger) failTest(level Level, msg string, fields ...zap.Field) {
	tl.state.t.Helper()
	tl.recordFailure(level, msg, fields)
	// skip captureStack, failTest, and the Logger function that called failTest
	tl.state.t.Fatalf("%s\n%s", failureMessage(level, msg, fields), captureStack(3))
}

// captureStack returns a formatted stack trace, skipping the first `skip` frames
// (including the captureStack frame itself).
func captureStack(skip int) string {
	pcs := make([]uintptr, 128) // 128 is likely larger than we'll ever need.
	frameCount := runtime.Callers(skip, pcs)

	// runtime.Callers truncates the recorded stacktrace to fit the provided slice.
	// Just keep doubling in size until we have enough space.
	for frameCount == len(pcs) {
		pcs = make([]uintptr, len(pcs)*2)
		frameCount = runtime.Callers(skip, pcs)
	}

	stackFrames := runtime.CallersFrames(pcs[:frameCount])
	var stackTrace strings.Builder
	for {
		frame, more := stackFrames.Next()
		fmt.Fprintf(&stackTrace, "%s:%d %s\n", frame.File, frame.Line, frame.Function)
		if !more {
			break
		}
	}
	return stackTrace.String()
}

// WithFields gives you a new logger, copying the fields of the source, appending the provided new Fields
func (tl *TestLogger) WithFields(fields ...zap.Field) *TestLogger {
	allFields := make([]zap.Field, len(tl.fields)+len(fields))
	copy(allFields, tl.fields)
	copy(allFields[len(tl.fields):], fields)
	newtl := &TestLogger{
		wrapped: tl.wrapped,
		state:   tl.state,
		fields:  allFields,
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

// With implements Logger
func (tl *TestLogger) With(fields ...zap.Field) *TestLogger {
	return &TestLogger{
		wrapped: tl.wrapped,
		state:   tl.state,
		fields:  tl.mergeWithLoggerFields(fields),
	}
}

// Format the Logger fields and such into a useful message
func failureMessage(level Level, msg string, fields []zap.Field) string {
	var b strings.Builder
	b.WriteString("FAIL: ")
	b.WriteString(errorMessage(level, msg))
	for _, t := range fields {
		b.WriteString(t.Key)
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

func message(level Level, msg string, fields []zap.Field) string {
	var b strings.Builder
	b.WriteString(level.String())
	b.WriteRune(':')
	b.WriteString(msg)
	b.WriteString("\" ")
	for _, t := range fields {
		b.WriteString(t.Key)
		b.WriteString("=")
		b.WriteString(formatValue(t))
		b.WriteString(" ")
	}
	return b.String()
}

func formatValue(t zap.Field) string {
	switch v := fieldValue(t).(type) {
	case fmt.Stringer:
		return v.String()
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func fieldValue(field zap.Field) any {
	encoder := zapcore.NewMapObjectEncoder()
	field.AddTo(encoder)
	return encoder.Fields[field.Key]
}
