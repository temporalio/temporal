package junit

import (
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	junitxml "github.com/jstemmer/go-junit-report/v2/junit"
)

// Testsuites is a JUnit test-suite collection.
type Testsuites = junitxml.Testsuites

// Testsuite is a JUnit test suite.
type Testsuite = junitxml.Testsuite

// Testcase is a JUnit test case.
type Testcase = junitxml.Testcase

// Result is a JUnit test-case failure or error.
type Result = junitxml.Result

var errRead = errors.New("failed to read JUnit report file")

var retrySuffixRe = regexp.MustCompile(` \(retry \d+\)$`)

// Read reads a JUnit XML file with either a testsuites or testsuite root.
func Read(path string) (*Testsuites, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open JUnit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	decoder := xml.NewDecoder(f)
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errRead, err)
		}
		root, ok := token.(xml.StartElement)
		if !ok {
			continue
		}

		switch root.Name.Local {
		case "testsuites":
			var testsuites Testsuites
			if err := decoder.DecodeElement(&testsuites, &root); err != nil {
				return nil, fmt.Errorf("%w: %w", errRead, err)
			}
			return &testsuites, nil
		case "testsuite":
			var testsuite Testsuite
			if err := decoder.DecodeElement(&testsuite, &root); err != nil {
				return nil, fmt.Errorf("%w: %w", errRead, err)
			}
			testsuites := &Testsuites{Time: testsuite.Time}
			testsuites.AddSuite(testsuite)
			return testsuites, nil
		default:
			return nil, fmt.Errorf("%w: unexpected root element %q", errRead, root.Name.Local)
		}
	}
}

// ReadTestcases reads and flattens all test cases in a JUnit XML file.
func ReadTestcases(path string) ([]Testcase, error) {
	testsuites, err := Read(path)
	if err != nil {
		return nil, err
	}

	var cases []Testcase
	for _, suite := range testsuites.Suites {
		cases = append(cases, suite.Testcases...)
	}
	return cases, nil
}

// LeafTestDurations returns the longest observed duration for each non-skipped leaf test. Retry
// suffixes are removed, and parent durations are omitted because they include their subtests.
func LeafTestDurations(cases []Testcase) map[string]float64 {
	observed := make(map[string]float64, len(cases))
	for _, tc := range cases {
		if tc.Skipped != nil {
			continue
		}
		name := normalizeTestName(tc.Name)
		if name == "" {
			continue
		}
		seconds, err := strconv.ParseFloat(tc.Time, 64)
		if err != nil || seconds < 0 {
			seconds = 0
		}
		observed[name] = max(observed[name], seconds)
	}

	names := slices.Sorted(maps.Keys(observed))
	durations := make(map[string]float64, len(observed))
	for i, name := range names {
		if i+1 < len(names) && strings.HasPrefix(names[i+1], name+"/") {
			continue
		}
		durations[name] = observed[name]
	}
	return durations
}

func normalizeTestName(name string) string {
	name = strings.TrimSuffix(name, " (final)")
	return retrySuffixRe.ReplaceAllString(name, "")
}

// Write writes a JUnit XML file.
func Write(path string, testsuites *Testsuites) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create JUnit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err := encoder.Encode(testsuites); err != nil {
		return fmt.Errorf("failed to write JUnit report file: %w", err)
	}
	return nil
}
