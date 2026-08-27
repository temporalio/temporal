package mixedbrain

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"go.temporal.io/server/common/util"
)

const omesSynopsisMaxFieldBytes = 2 << 10

type omesLogEntry struct {
	Level     string `json:"L"`
	Timestamp string `json:"T"`
	Message   string `json:"M"`
}

type omesFailureCause struct {
	message   string
	count     int
	firstSeen string
}

type omesFailureSynopsis struct {
	finalError  string
	likelyCause omesFailureCause
}

func newOmesFailure(scenario, logPath string, commandErr error) error {
	synopsis, err := summarizeOmesFailure(logPath)
	if err != nil {
		return fmt.Errorf("Omes %s failed:\n  command error: %v\n  log summary error: %v\n  full log: %s",
			scenario, commandErr, err, logPath)
	}

	var message strings.Builder
	fmt.Fprintf(&message, "Omes %s failed:\n", scenario)
	if synopsis.finalError != "" {
		fmt.Fprintf(&message, "  final error: %s\n", synopsis.finalError)
	} else {
		fmt.Fprintf(&message, "  command error: %v\n", commandErr)
	}
	if synopsis.likelyCause.message != "" {
		fmt.Fprintf(&message, "  likely cause: %s (%d occurrences)\n",
			synopsis.likelyCause.message, synopsis.likelyCause.count)
		if synopsis.likelyCause.firstSeen != "" {
			fmt.Fprintf(&message, "  first seen: %s\n", synopsis.likelyCause.firstSeen)
		}
	}
	fmt.Fprintf(&message, "  full log: %s", logPath)
	return fmt.Errorf("%s", message.String())
}

func summarizeOmesFailure(logPath string) (omesFailureSynopsis, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return omesFailureSynopsis{}, err
	}
	defer func() { _ = f.Close() }()

	causes := make(map[string]*omesFailureCause)
	var synopsis omesFailureSynopsis
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var entry omesLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		switch strings.ToLower(entry.Level) {
		case "fatal":
			synopsis.finalError = truncateSynopsisField(entry.Message)
		case "error":
			cause := omesRootCause(entry.Message)
			if cause == "" || isCancellationError(cause) {
				continue
			}
			candidate := causes[cause]
			if candidate == nil {
				candidate = &omesFailureCause{message: cause, firstSeen: normalizeLogTime(entry.Timestamp)}
				causes[cause] = candidate
			}
			candidate.count++
			if candidate.count > synopsis.likelyCause.count {
				synopsis.likelyCause = *candidate
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return omesFailureSynopsis{}, err
	}
	return synopsis, nil
}

func omesRootCause(message string) string {
	message = strings.TrimSpace(message)
	if index := strings.LastIndex(message, " desc = "); index >= 0 {
		message = message[index+len(" desc = "):]
	} else if index := strings.LastIndex(message, ": "); index >= 0 {
		message = message[index+2:]
	}
	return truncateSynopsisField(strings.TrimSpace(message))
}

func isCancellationError(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "context canceled") || strings.Contains(lower, "context deadline exceeded")
}

func normalizeLogTime(value string) string {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return value
	}
	return parsed.UTC().Format(time.RFC3339)
}

func truncateSynopsisField(value string) string {
	return util.TruncateUTF8(value, omesSynopsisMaxFieldBytes)
}
