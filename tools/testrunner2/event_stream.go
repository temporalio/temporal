package testrunner2

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"time"
)

type testEvent struct {
	Action string // "run", "pass", "fail", "skip"
	Test   string // "TestFoo" or "TestFoo/Sub1"
}

type testEventStreamConfig struct {
	Writer            io.Writer          // underlying writer (logCapture)
	Handler           func(testEvent)    // called for each lifecycle event
	StuckThreshold    time.Duration      // per-test: cancel when a single test runs longer than this (0 = disabled)
	AllStuckThreshold time.Duration      // all-stuck: cancel when no new test started within this time (0 = disabled)
	StuckCancel       context.CancelFunc // cancel when stuck detected
	Log               func(format string, v ...any)
}

type testEventStream struct {
	w       io.Writer
	handler func(testEvent)

	// Stuck test monitoring
	threshold         time.Duration      // per-test stuck threshold
	allStuckThreshold time.Duration      // all-stuck threshold (no new test started)
	cancel            context.CancelFunc // cancel when stuck detected
	log               func(format string, v ...any)

	mu              sync.Mutex
	running         map[string]time.Time
	lastTestStarted time.Time // time of most recent === RUN event
	lineBuf         []byte
	stuckNames      []string      // test names detected as stuck
	stuckDur        time.Duration // duration of the longest stuck test
	closed          chan struct{}
}

func newTestEventStream(cfg testEventStreamConfig) *testEventStream {
	s := &testEventStream{
		w:                 cfg.Writer,
		handler:           cfg.Handler,
		threshold:         cfg.StuckThreshold,
		allStuckThreshold: cfg.AllStuckThreshold,
		cancel:            cfg.StuckCancel,
		log:               cfg.Log,
		running:           make(map[string]time.Time),
		lastTestStarted:   time.Now(), // initialized to creation time so monitor works even if no tests start
		closed:            make(chan struct{}),
	}
	if s.threshold > 0 || s.allStuckThreshold > 0 {
		go s.watchLoop()
	}
	return s
}

// Write passes data through to the underlying writer while parsing test events.
func (s *testEventStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.lineBuf = append(s.lineBuf, p...)
	s.processLines()
	s.mu.Unlock()
	return s.w.Write(p)
}

func (s *testEventStream) processLines() {
	for {
		idx := bytes.IndexByte(s.lineBuf, '\n')
		if idx < 0 {
			break
		}
		line := s.lineBuf[:idx]
		s.lineBuf = s.lineBuf[idx+1:]
		s.parseLine(line)
	}
}

// parseLine extracts test lifecycle events from verbose test output.
// Handles both plain `go test -v` output and `-test.v=test2json` format
// (which prefixes event lines with 0x16 SYN characters).
func (s *testEventStream) parseLine(line []byte) {
	str := strings.TrimLeft(string(line), "\x16")
	str = strings.TrimSpace(str)

	fields := strings.Fields(str)
	if len(fields) < 3 {
		return
	}

	var ev testEvent
	switch {
	case fields[0] == "===" && fields[1] == "RUN":
		ev = testEvent{Action: "run", Test: fields[2]}
		now := time.Now()
		s.running[ev.Test] = now
		s.lastTestStarted = now
	case fields[0] == "---" && fields[1] == "PASS:":
		ev = testEvent{Action: "pass", Test: fields[2]}
		delete(s.running, ev.Test)
	case fields[0] == "---" && fields[1] == "FAIL:":
		ev = testEvent{Action: "fail", Test: fields[2]}
		delete(s.running, ev.Test)
	case fields[0] == "---" && fields[1] == "SKIP:":
		ev = testEvent{Action: "skip", Test: fields[2]}
		delete(s.running, ev.Test)
	default:
		return
	}

	if s.handler != nil {
		s.handler(ev)
	}
}

func (s *testEventStream) watchLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-ticker.C:
			if names, dur := s.checkStuck(); len(names) > 0 {
				if len(names) == 1 {
					s.log("stuck test detected: %s (running for %v)", names[0], dur.Round(time.Second))
				} else {
					s.log("all remaining tests appear stuck (no new test started for %v): %v", dur.Round(time.Second), names)
				}
				s.cancel()
				return
			}
		}
	}
}

func (s *testEventStream) checkStuck() ([]string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.stuckNames) > 0 {
		return nil, 0 // already detected
	}
	now := time.Now()

	// Per-test stuck check: any individual test running longer than threshold
	if s.threshold > 0 {
		for name, started := range s.running {
			dur := now.Sub(started)
			if dur > s.threshold {
				if s.hasRunningChildren(name) {
					continue
				}
				s.stuckNames = []string{name}
				s.stuckDur = dur
				return s.stuckNames, dur
			}
		}
	}

	// All-stuck check: no new test started for > allStuckThreshold
	if s.allStuckThreshold > 0 && !s.lastTestStarted.IsZero() {
		dur := now.Sub(s.lastTestStarted)
		if dur > s.allStuckThreshold && len(s.running) > 0 {
			var names []string
			for name := range s.running {
				if !s.hasRunningChildren(name) {
					names = append(names, name)
				}
			}
			if len(names) > 0 {
				s.stuckNames = names
				s.stuckDur = dur
				return names, dur
			}
		}
	}

	return nil, 0
}

func (s *testEventStream) hasRunningChildren(name string) bool {
	prefix := name + "/"
	for other := range s.running {
		if strings.HasPrefix(other, prefix) {
			return true
		}
	}
	return false
}

// StuckTests returns the names and duration of stuck tests, if any were detected.
// Returns a single test for per-test stuck detection, or all remaining tests for
// all-stuck detection (no new test started within the threshold).
func (s *testEventStream) StuckTests() ([]string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stuckNames, s.stuckDur
}

// Close stops the monitoring goroutine.
func (s *testEventStream) Close() {
	close(s.closed)
}
