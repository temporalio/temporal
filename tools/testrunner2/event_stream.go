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
	Writer         io.Writer          // underlying writer (logCapture)
	Handler        func(testEvent)    // called for each lifecycle event
	StuckThreshold time.Duration      // 0 = disabled
	StuckCancel    context.CancelFunc // cancel when stuck detected
	Log            func(format string, v ...any)
}

type testEventStream struct {
	w       io.Writer
	handler func(testEvent)

	// Stuck test monitoring (carried over from stuckTestMonitor)
	threshold time.Duration
	cancel    context.CancelFunc
	log       func(format string, v ...any)

	mu        sync.Mutex
	running   map[string]time.Time
	lineBuf   []byte
	stuckName string
	stuckDur  time.Duration
	closed    chan struct{}
}

func newTestEventStream(cfg testEventStreamConfig) *testEventStream {
	s := &testEventStream{
		w:         cfg.Writer,
		handler:   cfg.Handler,
		threshold: cfg.StuckThreshold,
		cancel:    cfg.StuckCancel,
		log:       cfg.Log,
		running:   make(map[string]time.Time),
		closed:    make(chan struct{}),
	}
	if s.threshold > 0 {
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
		s.running[ev.Test] = time.Now()
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
			if name, dur := s.checkStuck(); name != "" {
				s.log("stuck test detected: %s (running for %v)", name, dur.Round(time.Second))
				s.cancel()
				return
			}
		}
	}
}

func (s *testEventStream) checkStuck() (string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stuckName != "" {
		return "", 0 // already detected
	}
	now := time.Now()
	for name, started := range s.running {
		dur := now.Sub(started)
		if dur > s.threshold {
			if s.hasRunningChildren(name) {
				continue
			}
			s.stuckName = name
			s.stuckDur = dur
			return name, dur
		}
	}
	return "", 0
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

// StuckTest returns the name and duration of the stuck test, if one was detected.
func (s *testEventStream) StuckTest() (string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stuckName, s.stuckDur
}

// Close stops the monitoring goroutine.
func (s *testEventStream) Close() {
	close(s.closed)
}
