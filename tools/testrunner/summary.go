package testrunner

import (
	"encoding/json"
	"fmt"
	"html"
	"slices"
	"strings"
)

const summaryMaxDetailBytes = 4 * 1024
const summaryMarkdownMaxBytes = 1000 * 1024
const summaryTruncatedMarker = "\n… (truncated - see full output in job logs)\n"

type summary struct {
	Rows []summaryRow `json:"rows"`
}

func newSummaryFromReports(reports []*junitReport) summary {
	return summary{
		Rows: newSummaryRowsFromReports(reports),
	}
}

// Markdown renders the GitHub step summary HTML and enforces both the total
// summary budget and per-row detail truncation.
func (s summary) Markdown() string {
	if len(s.Rows) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("<table>\n<tr><th>Kind</th><th>Test</th></tr>\n")

	// Reserve bytes for the closing tag so we can always finish the table.
	const tableClose = "</table>\n"
	budget := summaryMarkdownMaxBytes - sb.Len() - len(tableClose)

	written := 0
	for _, row := range s.Rows {
		rendered := row.Markdown()
		if len(rendered) > budget {
			omitted := len(s.Rows) - written
			fmt.Fprintf(&sb, "<tr><td colspan=\"2\">… %d failure(s) not shown — see full output in job logs</td></tr>\n", omitted)
			break
		}
		sb.WriteString(rendered)
		budget -= len(rendered)
		written++
	}

	sb.WriteString(tableClose)
	return sb.String()
}

func (s summary) JSON() ([]byte, error) {
	return json.MarshalIndent(s, "", "  ")
}

// summaryRow is a single failure entry rendered in both Markdown and JSON summaries.
type summaryRow struct {
	Kind    failureType `json:"kind"`
	Name    string      `json:"name"`              // test or alert name
	Details string      `json:"details,omitempty"` // failure body
	Final   bool        `json:"final,omitempty"`
}

func newSummaryRowsFromReports(reports []*junitReport) []summaryRow {
	var rows []summaryRow
	for _, report := range reports {
		for _, suite := range report.Suites {
			for _, tc := range suite.Testcases {
				if tc.Failure == nil {
					continue
				}
				rows = append(rows, newSummaryRow(failureType(tc.Failure.Type), tc.Name, tc.Failure.Data))
			}
		}
	}
	slices.SortFunc(rows, func(a, b summaryRow) int {
		if byName := strings.Compare(a.Name, b.Name); byName != 0 {
			return byName
		}
		if byKind := strings.Compare(string(a.Kind), string(b.Kind)); byKind != 0 {
			return byKind
		}
		return strings.Compare(a.Details, b.Details)
	})
	return rows
}

func newSummaryRow(kind failureType, name string, details string) summaryRow {
	if len(details) > summaryMaxDetailBytes {
		headBytes := (summaryMaxDetailBytes - len(summaryTruncatedMarker)) / 2
		tailBytes := summaryMaxDetailBytes - len(summaryTruncatedMarker) - headBytes
		details = details[:headBytes] + summaryTruncatedMarker + details[len(details)-tailBytes:]
	}
	return summaryRow{
		Kind:    kind,
		Name:    name,
		Details: details,
		Final:   strings.Contains(name, "(final)"),
	}
}

// Markdown renders one summary table row.
func (row summaryRow) Markdown() string {
	kind := string(row.Kind)
	if row.Final {
		kind = "❌ " + kind
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "<tr><td>%s</td><td>", html.EscapeString(kind))
	if row.Details != "" {
		escaped := html.EscapeString(row.Details)
		if strings.Contains(row.Details, summaryTruncatedMarker) {
			escaped += "\n… (truncated — see full output in job logs)"
		}
		fmt.Fprintf(&sb, "<details><summary>%s</summary><pre>%s</pre></details>",
			html.EscapeString(row.Name), escaped)
	} else {
		sb.WriteString(html.EscapeString(row.Name))
	}
	sb.WriteString("</td></tr>\n")
	return sb.String()
}
