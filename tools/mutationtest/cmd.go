package mutationtest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

var (
	stdoutLogger = log.New(os.Stdout, "", log.Ltime)
	stderrLogger = log.New(os.Stderr, "", log.Ltime)
)

func labeledCommand(ctx context.Context, prefix string, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	if strings.TrimSpace(prefix) == "" {
		prefix = os.Getenv("MUTATION_OUTPUT_PREFIX")
	}
	if strings.TrimSpace(prefix) == "" {
		return cmd
	}
	cmd.Stdout = newOutputWriter(os.Stdout, prefix+" ")
	cmd.Stderr = newOutputWriter(os.Stderr, prefix+" ")
	return cmd
}

type outputWriter struct {
	buf    bytes.Buffer
	mu     sync.Mutex
	prefix string
	w      io.Writer
}

func newOutputWriter(w io.Writer, prefix string) *outputWriter {
	return &outputWriter{prefix: prefix, w: w}
}

func (w *outputWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, b := range p {
		if err := w.buf.WriteByte(b); err != nil {
			return 0, err
		}
		if b != '\n' && b != '\r' {
			continue
		}
		if _, err := fmt.Fprint(w.w, w.prefix, w.buf.String()); err != nil {
			return 0, err
		}
		w.buf.Reset()
	}
	return len(p), nil
}
