package engine

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
)

// Field supplies structured log data without choosing a logging backend.
type Field interface {
	Key() string
	Value() any
}

// Logger writes the log calls observed by Engine.
type Logger[F Field] interface {
	Debug(string, ...F)
	Info(string, ...F)
	Warn(string, ...F)
	Error(string, ...F)
	DPanic(string, ...F)
	Panic(string, ...F)
	Fatal(string, ...F)
}

type Option[F Field] func(*Engine[F])

// CallerSkip accounts for adapters between Engine and the calling test.
func CallerSkip[F Field](skip int) Option[F] {
	return func(t *Engine[F]) { t.state.callerSkip = skip }
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
	// like an allowlist. Use Engine.Expect to add an error to the allowlist.
	FailOnAnyUnexpectedError Mode = "fail-on-unexpected-errors"
	// FailOnExpectedErrorOnly mode will only fail if an expected error is encountered,
	// like a blocklist. Use Engine.Expect to add an error to the blocklist.
	FailOnExpectedErrorOnly = "fail-on-expected-errors"
)

// This is a subset of the testing.T interface that we use in this package that is
// also shared with *rapid.T.
type TestingT interface {
	Errorf(string, ...any)
	Fail()
	Failed() bool
	FailNow()
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

// Expectation represents a log call we expect to happen in tests.
// For error-level logs, expectations also influence whether the log fails the
// test based on the logger mode.
type Expectation[F Field] struct {
	e          *list.Element
	testLogger *Engine[F]
	lvl        Level
	matches    atomic.Int64
}

// Forget removes a previously registered expectation.
// A forgotten expectation will no longer be evaluated when logs are encountered.
func (e *Expectation[F]) Forget() {
	e.testLogger.Forget(e)
}

func (e *Expectation[F]) Matched() bool {
	return e.MatchCount() > 0
}

func (e *Expectation[F]) MatchCount() int64 {
	return e.matches.Load()
}

type matcher[F Field] struct {
	expectation *Expectation[F]
	msg         *regexp.Regexp
	tags        map[string]*regexp.Regexp
}

func newMatcher[F Field](msg string, tags []F, e *Expectation[F]) matcher[F] {
	var rgx *regexp.Regexp
	if msg != "" {
		rgx = regexp.MustCompile(msg)
	}
	m := matcher[F]{
		expectation: e,
		msg:         rgx,
		tags:        map[string]*regexp.Regexp{},
	}
	for _, t := range tags {
		m.tags[t.Key()] = regexp.MustCompile(formatValue(t))
	}
	return m
}

func (m matcher[F]) Matches(msg string, tags []F) bool {
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

type sharedEngineState[F Field] struct {
	failOnDPanic atomic.Bool
	failOnError  atomic.Bool
	failOnFatal  atomic.Bool
	t            TestingT
	mu           struct {
		sync.RWMutex
		expectations map[Level]*list.List // Map[Level]List[matcher[F]]
		captures     map[*Capture[F]]struct{}
		closed       bool
	}
	mode            Mode
	logExpectations bool
	callerSkip      int
	failure         atomic.Pointer[Failure[F]]
}

// Failure describes a log call that shouldFailTest considered a test-failing log.
type Failure[F Field] struct {
	Level Level
	Msg   string
	Tags  []F
	Stack string
}

// Engine is a Logger implementation that logs to the test's logger
// _but_ will fail the test if log levels _above_ Warn are present
type Engine[F Field] struct {
	wrapped Logger[F]
	state   *sharedEngineState[F]
	tags    []F
}

func DontFailOnError[F Field](t *Engine[F]) *Engine[F] {
	t.state.failOnError.Store(false)
	return t
}

func DontFailOnDPanic[F Field](t *Engine[F]) *Engine[F] {
	t.state.failOnDPanic.Store(false)
	return t
}

func LogExpectations[F Field](t *Engine[F]) *Engine[F] {
	t.state.logExpectations = true
	return t
}

// WithLogTags adds tags to the logs emitted by the underlying logger
func WithLogTags[F Field](tags ...F) Option[F] {
	return func(t *Engine[F]) {
		t.tags = tags
	}
}

// WrapLogger sets the output logger used by the engine.
func WrapLogger[F Field](logger Logger[F]) Option[F] {
	return func(t *Engine[F]) { t.wrapped = logger }
}

// NewEngine creates a test logger using the supplied output logger.
func NewEngine[F Field](t TestingT, mode Mode, wrapped Logger[F], opts ...Option[F]) *Engine[F] {
	tl := &Engine[F]{wrapped: wrapped, state: &sharedEngineState[F]{t: t, mode: mode}}
	tl.state.mu.expectations = make(map[Level]*list.List)
	tl.state.mu.captures = make(map[*Capture[F]]struct{})
	tl.state.failOnError.Store(true)
	tl.state.failOnDPanic.Store(true)
	tl.state.failOnFatal.Store(true)
	for _, opt := range opts {
		opt(tl)
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
// level, msg, and tag arguments. For error-level logs, depending on the Mode of
// the test logger, the expectation either acts as an entry in a blocklist
// (FailOnExpectedErrorOnly) or an allowlist (FailOnAnyUnexpectedError).
func (tl *Engine[F]) Expect(level Level, msg string, tags ...F) *Expectation[F] {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	if tl.state.logExpectations {
		tl.wrapped.Info(fmt.Sprintf("(%p) TestLogger::Expecting: '%s'\n", tl, msg))
	}
	e := &Expectation[F]{
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
// A forgotten expectation will no longer be evaluated when logs are encountered.
func (tl *Engine[F]) Forget(e *Expectation[F]) {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	tl.state.mu.expectations[e.lvl].Remove(e.e)
}

// StartCapture starts recording log calls that contain any of the provided exact tags.
// It records all calls when no tags are provided.
func (tl *Engine[F]) StartCapture(anyTags ...F) *Capture[F] {
	capture := newCapture(anyTags)
	tl.state.mu.Lock()
	tl.state.mu.captures[capture] = struct{}{}
	tl.state.mu.Unlock()
	return capture
}

// StopCapture stops recording log calls for capture.
func (tl *Engine[F]) StopCapture(capture *Capture[F]) {
	tl.state.mu.Lock()
	delete(tl.state.mu.captures, capture)
	tl.state.mu.Unlock()
}

func (tl *Engine[F]) shouldFailTest(level Level, msg string, tags []F) bool {
	// Only check expectations if they've been registered for this level.
	if expectations, found := tl.state.mu.expectations[level]; found {
		for e := expectations.Front(); e != nil; e = e.Next() {
			m, ok := e.Value.(matcher[F])
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

// recordExpectationMatches increments the match counter of every expectation
// registered at the given level whose matcher matches this log call. It is purely
// observational: it never affects whether the test fails (that remains the sole
// job of shouldFailTest), so it is safe to call for any log at any level.
func (tl *Engine[F]) recordExpectationMatches(level Level, msg string, tags []F) {
	expectations, found := tl.state.mu.expectations[level]
	if !found {
		return
	}
	for e := expectations.Front(); e != nil; e = e.Next() {
		m, ok := e.Value.(matcher[F])
		if !ok {
			tl.state.t.Fatalf("Bug in TestLogger: invalid %T value in matcher list", e.Value)
		}
		if m.Matches(msg, tags) {
			m.expectation.matches.Add(1)
		}
	}
}

func (tl *Engine[F]) recordCaptures(level Level, msg string, tags []F) {
	if len(tl.state.mu.captures) == 0 {
		return
	}
	record := CapturedLog[F]{
		Level:   level,
		Message: msg,
		Tags:    slices.Clone(tags),
	}
	for capture := range tl.state.mu.captures {
		capture.record(record)
	}
}

// FailOnDPanic overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
func (tl *Engine[F]) FailOnDPanic(b bool) bool {
	return tl.state.failOnDPanic.Swap(b)
}

// FailOnError overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
func (tl *Engine[F]) FailOnError(b bool) bool {
	return tl.state.failOnError.Swap(b)
}

// FailOnFatal overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
// Note that Fatal-level logs still panic
func (tl *Engine[F]) FailOnFatal(b bool) bool {
	return tl.state.failOnFatal.Swap(b)
}

// Failure returns the first Failure that shouldFailTest considered a test-failing
// log, or nil if no such log has been observed. Sticky: first failure wins.
func (tl *Engine[F]) Failure() *Failure[F] {
	return tl.state.failure.Load()
}

// recordFailure stores the first observed failure (first-failure-wins via CAS).
// Tags are copied so the recorded value is independent of the caller's slice.
func (tl *Engine[F]) recordFailure(level Level, msg string, tags []F) {
	if tl.state.failure.Load() != nil {
		// fast-path: already recorded; avoid the copy and the stack capture.
		return
	}
	tagsCopy := make([]F, len(tags))
	copy(tagsCopy, tags)
	f := &Failure[F]{
		Level: level,
		Msg:   msg,
		Tags:  tagsCopy,
		Stack: captureStack(3 + tl.state.callerSkip), // skip captureStack, recordFailure, and the Logger[F] method that called it
	}
	tl.state.failure.CompareAndSwap(nil, f)
}

func (tl *Engine[F]) mergeWithLoggerTags(tags []F) []F {
	if len(tl.tags) == 0 {
		return tags
	}
	tagMap := make(map[string]F, len(tl.tags)+len(tags))
	// Iterate over the logger's tags first so that explicitly specified tags override them
	for _, t := range tl.tags {
		tagMap[t.Key()] = t
	}
	for _, t := range tags {
		tagMap[t.Key()] = t
	}
	newTags := make([]F, 0, len(tagMap))
	for _, t := range tagMap {
		newTags = append(newTags, t)
	}
	slices.SortStableFunc(newTags, func(a, b F) int {
		return cmp.Compare(a.Key(), b.Key())
	})
	return newTags
}

// DPanic implements Logger.
func (tl *Engine[F]) DPanic(msg string, tags ...F) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.recordExpectationMatches(DPanic, msg, tags)
	tl.recordCaptures(DPanic, msg, tags)
	// note, actual panic'ing in wrapped is turned off so we can control.
	tl.wrapped.DPanic(msg, tags...)
	if tl.state.failOnDPanic.Load() && tl.shouldFailTest(DPanic, msg, tags) {
		tl.state.t.Helper()
		tl.failTest(DPanic, msg, tags...)
	}
}

// Debug implements Logger.
func (tl *Engine[F]) Debug(msg string, tags ...F) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.recordExpectationMatches(Debug, msg, tags)
	tl.recordCaptures(Debug, msg, tags)
	tl.wrapped.Debug(msg, tags...)
}

// Error implements Logger.
func (tl *Engine[F]) Error(msg string, tags ...F) {
	tl.state.mu.RLock()
	if tl.state.mu.closed {
		tl.state.mu.RUnlock()
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.recordExpectationMatches(Error, msg, tags)
	tl.recordCaptures(Error, msg, tags)
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

// Fatal implements Logger.
func (tl *Engine[F]) Fatal(msg string, tags ...F) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.recordExpectationMatches(Fatal, msg, tags)
	tl.recordCaptures(Fatal, msg, tags)
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

// Info implements Logger.
func (tl *Engine[F]) Info(msg string, tags ...F) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.recordExpectationMatches(Info, msg, tags)
	tl.recordCaptures(Info, msg, tags)
	tl.wrapped.Info(msg, tags...)
}

// Panic implements Logger.
func (tl *Engine[F]) Panic(msg string, tags ...F) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.recordExpectationMatches(Panic, msg, tags)
	tl.recordCaptures(Panic, msg, tags)
	tl.state.t.Helper()
	// Forcibly fail the test when required as otherwise panics can be caught.
	if tl.shouldFailTest(Panic, msg, tags) {
		tl.state.t.Helper()
		tl.failTest(Panic, msg, tags...)
	}
	tl.wrapped.Panic(message(Panic, msg, tags))
}

// Warn implements Logger.
func (tl *Engine[F]) Warn(msg string, tags ...F) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	tags = tl.mergeWithLoggerTags(tags)
	tl.recordExpectationMatches(Warn, msg, tags)
	tl.recordCaptures(Warn, msg, tags)
	tl.wrapped.Warn(msg, tags...)
}

// failTest fails the test while dumping the stack, allowing us to know where in the code
// the failure arose.
func (tl *Engine[F]) failTest(level Level, msg string, tags ...F) {
	tl.state.t.Helper()
	tl.recordFailure(level, msg, tags)
	// skip captureStack, failTest, and the Logger function that called failTest
	tl.state.t.Fatalf("%s\n%s", failureMessage(level, msg, tags), captureStack(3+tl.state.callerSkip))
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

// WithTags gives you a new logger, copying the tags of the source, appending the provided new Tags
func (tl *Engine[F]) WithTags(tags ...F) *Engine[F] {
	allTags := make([]F, len(tl.tags)+len(tags))
	copy(allTags, tl.tags)
	copy(allTags[len(tl.tags):], tags)
	newtl := &Engine[F]{
		wrapped: tl.wrapped,
		state:   tl.state,
		tags:    allTags,
	}
	return newtl
}

// Close disallows any further logging, preventing the test framework from complaining about
// logging post-test.
func (tl *Engine[F]) Close() {
	// Taking the write lock ensures all in-progress log calls complete before we close.
	// This prevents a race condition after the test has completed.
	tl.state.mu.Lock()
	tl.state.mu.closed = true
	tl.state.mu.Unlock()
}

func (tl *Engine[F]) T() TestingT {
	return tl.state.t
}

// With implements Logger
func (tl *Engine[F]) With(tags ...F) *Engine[F] {
	return &Engine[F]{
		wrapped: tl.wrapped,
		state:   tl.state,
		tags:    tl.mergeWithLoggerTags(tags),
	}
}

// Format the Logger tags and such into a useful message
func failureMessage[F Field](level Level, msg string, tags []F) string {
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

func message[F Field](level Level, msg string, tags []F) string {
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

func formatValue[F Field](t F) string {
	switch v := t.Value().(type) {
	case fmt.Stringer:
		return v.String()
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
