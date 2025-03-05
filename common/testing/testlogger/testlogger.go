// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package testlogger

import (
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
	unexpected bool
}

// Forget removes a previously registered expectation.
// It will no longer be used to determine if a log message is expected or unexpected.
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

type SharedTestLoggerState struct {
	failOnDPanic atomic.Bool
	failOnError  atomic.Bool
	t            TestingT
	mu           struct {
		sync.RWMutex
		expectations   map[Level]*list.List // Map[Level]List[matcher]
		closed         bool
		unexpectedErrs bool
	}
	logExpectations bool
	logCaller       bool
	level           zapcore.Level
}

// TestLogger is a log.Logger implementation that logs to the test's logger
// _but_ will fail the test if log levels _above_ Warn are present
type TestLogger struct {
	wrapped log.Logger
	state   *SharedTestLoggerState
	tags    []tag.Tag
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
// By default it will fail the test if Error, or DPanic is called.
func NewTestLogger(t TestingT, opts ...LoggerOption) *TestLogger {
	tl := &TestLogger{
		state: &SharedTestLoggerState{
			t:               t,
			logExpectations: false,
			level:           zapcore.DebugLevel,
			logCaller:       true,
		},
	}
	tl.state.failOnError.Store(true)
	tl.state.failOnDPanic.Store(true)
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
		ztl := zaptest.NewLogger(t,
			zaptest.Level(tl.state.level),
			zaptest.WrapOptions(
				zap.AddStacktrace(zap.ErrorLevel),
				zap.WithCaller(tl.state.logCaller),
			),
		)
		tl.wrapped = log.NewZapLogger(ztl).Skip(1)
	}

	// Only possible with a *testing.T until *rapid.T supports `Cleanup`
	if ct, ok := t.(CleanupCapableT); ok {
		// NOTE(tim): We don't care about anything logged after the test completes. Sure, this is racy,
		// but it reduces the likelihood that we see stupid errors due to testing.T.Logf race conditions...
		ct.Cleanup(tl.Close)
	}

	return tl
}

// ResetUnexpectedErrors resets the unexpected error state, returning the previous value.
func (tl *TestLogger) ResetUnexpectedErrors() bool {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	prevUnexpectedLogs := tl.state.mu.unexpectedErrs
	tl.state.mu.unexpectedErrs = false
	return prevUnexpectedLogs
}

// Expect instructs the logger to expect certain errors, as specified by the msg and tag arguments.
// This is useful for ignoring certain errors that are expected.
func (tl *TestLogger) Expect(level Level, msg string, tags ...tag.Tag) *Expectation {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	if tl.state.logExpectations {
		tl.wrapped.Info(fmt.Sprintf("(%p) TestLogger::Expecting: '%s'\n", tl, msg))
	}
	return tl.addExpectationLocked(false, level, msg, tags...)
}

// DontExpect instructs the logger to _not_ expect certain errors, as specified by the msg and tag arguments.
// This is useful for ensuring that certain errors _do not_ occur during a test.
func (tl *TestLogger) DontExpect(level Level, msg string, tags ...tag.Tag) *Expectation {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	if tl.state.logExpectations {
		tl.wrapped.Info(fmt.Sprintf("(%p) TestLogger::NotExpecting: '%s'\n", tl, msg))
	}
	return tl.addExpectationLocked(true, level, msg, tags...)
}

func (tl *TestLogger) addExpectationLocked(unexpected bool, level Level, msg string, tags ...tag.Tag) *Expectation {
	e := &Expectation{
		testLogger: tl,
		lvl:        level,
		unexpected: unexpected,
	}
	m := newMatcher(msg, tags, e)
	e.e = tl.state.mu.expectations[level].PushBack(m)
	return e
}

// Forget removes a previously registered expectation.
// It will no longer be used to determine if a log message is expected or unexpected.
func (tl *TestLogger) Forget(e *Expectation) {
	tl.state.mu.Lock()
	defer tl.state.mu.Unlock()
	tl.state.mu.expectations[e.lvl].Remove(e.e)
}

func (tl *TestLogger) isUnexpected(level Level, msg string, tags []tag.Tag) bool {
	expectations := tl.state.mu.expectations[level]
	for e := expectations.Front(); e != nil; e = e.Next() {
		m, ok := e.Value.(matcher)
		if !ok {
			tl.state.t.Fatalf("Bug in TestLogger: invalid %T value in matcher list", e.Value)
		}
		if m.Matches(msg, tags) {
			return m.expectation.unexpected
		}
	}
	return false
}

// FailOnError overrides the behavior of this logger. It returns the previous value
// so that it can be restored later.
func (tl *TestLogger) FailOnError(v bool) bool {
	return tl.state.failOnError.Swap(v)
}

// DPanic implements log.Logger.
func (tl *TestLogger) DPanic(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	if tl.tags != nil {
		tags = append(tags, tl.tags...)
	}
	// note, actual panic'ing in wrapped is turned off so we can control.
	tl.wrapped.DPanic(msg, tags...)
	if tl.state.failOnDPanic.Load() && tl.isUnexpected(Error, msg, tags) {
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
	if tl.tags != nil {
		tags = append(tags, tl.tags...)
	}
	_ = tl.isUnexpected(Debug, msg, tags)
	tl.wrapped.Debug(msg, append(tags, tl.tags...)...)
}

// Error implements log.Logger.
func (tl *TestLogger) Error(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	if tl.state.mu.closed {
		tl.state.mu.RUnlock()
		return
	}
	if tl.tags != nil {
		tags = append(tags, tl.tags...)
	}
	if !tl.isUnexpected(Error, msg, tags) {
		tl.wrapped.Error(msg, tags...)
		tl.state.mu.RUnlock()
		return
	}
	tl.state.mu.RUnlock()

	if tl.state.failOnError.Load() {
		tl.state.t.Helper()
		tl.wrapped.Error(msg, tags...)
		panic(failureMessage("Error", msg, tags))
	}

	// Labeling the error as unexpected; so it can be found later.
	tl.wrapped.Error(errorMessage("Error", msg), tags...)
	tl.state.mu.Lock()
	tl.state.mu.unexpectedErrs = true
	tl.state.mu.Unlock()
}

// Fatal implements log.Logger.
func (tl *TestLogger) Fatal(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	if tl.tags != nil {
		tags = append(tags, tl.tags...)
	}
	tl.state.t.Helper()
	tl.state.t.Fatal(failureMessage("Fatal", msg, tags))
}

// Info implements log.Logger.
func (tl *TestLogger) Info(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	if tl.tags != nil {
		tags = append(tags, tl.tags...)
	}
	_ = tl.isUnexpected(Info, msg, tags)
	tl.wrapped.Info(msg, tags...)
}

// Panic implements log.Logger.
func (tl *TestLogger) Panic(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	if tl.tags != nil {
		tags = append(tags, tl.tags...)
	}
	tl.state.t.Helper()
	tl.state.t.Fatal(failureMessage("Panic", msg, tags))
}

// Warn implements log.Logger.
func (tl *TestLogger) Warn(msg string, tags ...tag.Tag) {
	tl.state.mu.RLock()
	defer tl.state.mu.RUnlock()
	if tl.state.mu.closed {
		return
	}
	if tl.tags != nil {
		tags = append(tags, tl.tags...)
	}
	_ = tl.isUnexpected(Warn, msg, tags)
	tl.wrapped.Warn(msg, tags...)
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
