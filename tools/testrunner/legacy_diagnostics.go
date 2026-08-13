package testrunner

import "strings"

// noFailureDetails is returned by parseFailureDetails when no recognisable
// failure block is found.
const noFailureDetails = "(error details not found)"

// alert captures a prominent issue detected from stdout/stderr of test runs.
type alert struct {
	Type    failureType
	Summary string
	Details string
	Tests   []string
}

// primaryTestName returns a single representative test name for an alert.
// Preference order:
// 1) Fully-qualified test name containing ".Test"
// 2) First detected test name
func primaryTestName(tests []string) string {
	if len(tests) == 0 {
		return ""
	}
	for _, test := range tests {
		if strings.Contains(test, ".Test") {
			return test
		}
	}
	return tests[0]
}

// parseAlerts scans a gotestsum/go test stdout stream and extracts high-priority
// alerts such as data races and panics. It returns a slice of alerts in the
// order they were encountered.
func parseAlerts(output string) []alert {
	diagnostics := extractDiagnostics(outputScope{}, output)
	alerts := make([]alert, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.kind == diagnosticTimeout {
			continue
		}
		alerts = append(alerts, alert{
			Type:    failureTypeForDiagnostic(diagnostic.kind),
			Summary: diagnostic.summary,
			Details: diagnostic.details,
			Tests:   extractTestNames(diagnostic.details),
		})
	}
	return alerts
}

// parseFailedTestsFromOutput extracts failing test names from gotestsum stdout.
// It looks for Go test failure lines produced as tests complete, and is
// used when the test binary was killed externally before producing a JUnit XML.
func parseFailedTestsFromOutput(output string) []string {
	var failed []string
	seen := make(map[string]struct{})
	for line := range strings.SplitSeq(strings.ReplaceAll(output, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, goTestFailLinePrefix) {
			continue
		}
		if name, ok := parseTripleDashTestName(line); ok {
			addUniqueTest(&failed, seen, name)
		}
	}
	return failed
}

// parseFailureDetails extracts the actionable part of a JUnit failure Data block.
func parseFailureDetails(data string) string {
	evidence := extractFailureEvidence(data)
	if evidence.details == "" {
		return noFailureDetails
	}
	return evidence.details
}
