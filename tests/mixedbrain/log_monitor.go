package mixedbrain

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const failedAssertionLogPrefix = "failed assertion: "

type logMonitor struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	findings []string
}

func newLogMonitor(t *testing.T) *logMonitor {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	m := &logMonitor{
		ctx:    ctx,
		cancel: cancel,
	}
	t.Cleanup(m.stop)
	return m
}

func (m *logMonitor) watch(t *testing.T, name, path string) {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		require.NoError(t, err)
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer f.Close()

		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadString('\n')
			if err == nil {
				m.checkLine(name, strings.TrimSpace(line))
				continue
			}
			if err != io.EOF {
				m.addFinding(name, fmt.Sprintf("failed reading log: %v", err))
				return
			}

			select {
			case <-m.ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
}

func (m *logMonitor) stop() {
	m.cancel()
	m.wg.Wait()
}

func (m *logMonitor) assertNoFindings(t *testing.T) {
	t.Helper()
	m.stop()

	m.mu.Lock()
	defer m.mu.Unlock()
	require.Empty(t, m.findings, "Temporal server soft assertions:\n%s", strings.Join(m.findings, "\n"))
}

func (m *logMonitor) checkLine(name, line string) {
	if line == "" {
		return
	}

	if strings.Contains(line, failedAssertionLogPrefix) {
		m.addFinding(name, line)
	}
}

func (m *logMonitor) addFinding(name, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.findings = append(m.findings, fmt.Sprintf("[%s] %s", name, msg))
}
