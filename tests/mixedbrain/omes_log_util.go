package mixedbrain

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.temporal.io/server/common/util"
)

const (
	omesSynopsisMaxFieldBytes = 2 << 10
	maxOmesErrorFindings      = 3
)

type omesLogEntry struct {
	Level     string `json:"L"`
	Timestamp string `json:"T"`
	Message   string `json:"M"`
}

type omesLogFinding struct {
	level     string
	message   string
	count     int
	firstSeen string
}

type omesFailureSynopsis struct {
	finalError    string
	errorFindings []omesLogFinding
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
	if findings := synopsis.formatErrorFindings(); findings != "" {
		fmt.Fprintf(&message, "  recurring errors:\n%s", findings)
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

	findings := omesErrorFindings{byKey: make(map[string]int)}
	var synopsis omesFailureSynopsis
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		var entry omesLogEntry
		if err := json.Unmarshal([]byte(line), &entry); err == nil {
			switch strings.ToLower(entry.Level) {
			case "fatal":
				synopsis.finalError = truncateSynopsisField(entry.Message)
			default:
				findings.add(entry.Level, entry.Timestamp, entry.Message)
			}
			continue
		}
		if timestamp, level, message, ok := parseWorkerLogError(line); ok {
			findings.add(level, timestamp, message)
		}
	}
	if err := scanner.Err(); err != nil {
		return omesFailureSynopsis{}, err
	}
	synopsis.errorFindings = findings.top(maxOmesErrorFindings)
	return synopsis, nil
}

type omesErrorFindings struct {
	findings []omesLogFinding
	byKey    map[string]int
}

func (f *omesErrorFindings) add(level, timestamp, message string) {
	level = strings.ToUpper(level)
	if level != "ERROR" {
		return
	}
	message = omesRootCause(message)
	if message == "" || isCancellationError(message) {
		return
	}
	key := level + "\x00" + message
	if index, ok := f.byKey[key]; ok {
		f.findings[index].count++
		return
	}
	f.byKey[key] = len(f.findings)
	f.findings = append(f.findings, omesLogFinding{
		level:     level,
		message:   message,
		count:     1,
		firstSeen: normalizeLogTime(timestamp),
	})
}

func (f omesErrorFindings) top(limit int) []omesLogFinding {
	sort.SliceStable(f.findings, func(i, j int) bool {
		return f.findings[i].count > f.findings[j].count
	})
	if len(f.findings) > limit {
		return f.findings[:limit]
	}
	return f.findings
}

func (s omesFailureSynopsis) formatErrorFindings() string {
	var out strings.Builder
	for _, finding := range s.errorFindings {
		fmt.Fprintf(&out, "    %s %s: %d occurrences", finding.level, finding.message, finding.count)
		if finding.firstSeen != "" {
			fmt.Fprintf(&out, " (first seen: %s)", finding.firstSeen)
		}
		out.WriteByte('\n')
	}
	return out.String()
}

func parseWorkerLogError(line string) (timestamp, level, message string, ok bool) {
	timestamp, remainder, ok := strings.Cut(line, "\t")
	if !ok {
		return "", "", "", false
	}
	level, remainder, ok = strings.Cut(remainder, "\t")
	if !ok {
		return "", "", "", false
	}
	payloadIndex := strings.LastIndex(remainder, "\t{")
	if payloadIndex < 0 {
		return "", "", "", false
	}
	var fields struct {
		Error string `json:"Error"`
	}
	if err := json.Unmarshal([]byte(remainder[payloadIndex+1:]), &fields); err != nil || fields.Error == "" {
		return "", "", "", false
	}
	return timestamp, level, fields.Error, true
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
