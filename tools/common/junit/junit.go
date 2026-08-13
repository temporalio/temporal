package junit

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// Output is captured JUnit test output.
type Output = junitxml.Output

var errRead = errors.New("failed to read JUnit report file")

// Read reads a JUnit XML file with either a testsuites or testsuite root.
func Read(path string) (*Testsuites, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open JUnit report file %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	decoder := xml.NewDecoder(f)
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("%w %q: %w", errRead, path, err)
		}
		root, ok := token.(xml.StartElement)
		if !ok {
			continue
		}

		switch root.Name.Local {
		case "testsuites":
			var testsuites Testsuites
			if err := decoder.DecodeElement(&testsuites, &root); err != nil {
				return nil, fmt.Errorf("%w %q: %w", errRead, path, err)
			}
			if err := validateDocumentEnd(decoder); err != nil {
				return nil, fmt.Errorf("%w %q: %w", errRead, path, err)
			}
			return &testsuites, nil
		case "testsuite":
			var testsuite Testsuite
			if err := decoder.DecodeElement(&testsuite, &root); err != nil {
				return nil, fmt.Errorf("%w %q: %w", errRead, path, err)
			}
			if err := validateDocumentEnd(decoder); err != nil {
				return nil, fmt.Errorf("%w %q: %w", errRead, path, err)
			}
			testsuites := &Testsuites{Time: testsuite.Time}
			testsuites.AddSuite(testsuite)
			return testsuites, nil
		default:
			return nil, fmt.Errorf("%w %q: unexpected root element %q", errRead, path, root.Name.Local)
		}
	}
}

func validateDocumentEnd(decoder *xml.Decoder) error {
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.CharData:
			if strings.TrimSpace(string(token)) != "" {
				return fmt.Errorf("unexpected trailing XML data %q", strings.TrimSpace(string(token)))
			}
		case xml.Comment, xml.ProcInst:
		case xml.StartElement:
			return fmt.Errorf("unexpected trailing XML element %q", token.Name.Local)
		default:
			return fmt.Errorf("unexpected trailing XML token %T", token)
		}
	}
}

// ValidateCounters verifies that suite and root counters describe the emitted
// testcase tree exactly.
func ValidateCounters(testsuites *Testsuites) error {
	if testsuites == nil {
		return errors.New("JUnit report is nil")
	}

	var rootTests, rootErrors, rootFailures, rootSkipped, rootDisabled int
	for i := range testsuites.Suites {
		suite := &testsuites.Suites[i]
		tests := len(suite.Testcases)
		var errorCount, failures, skipped int
		for _, testcase := range suite.Testcases {
			if testcase.Error != nil {
				errorCount++
			}
			if testcase.Failure != nil {
				failures++
			}
			if testcase.Skipped != nil {
				skipped++
			}
		}
		if err := validateCounter(fmt.Sprintf("suite %q tests", suite.Name), suite.Tests, tests); err != nil {
			return err
		}
		if err := validateCounter(fmt.Sprintf("suite %q errors", suite.Name), suite.Errors, errorCount); err != nil {
			return err
		}
		if err := validateCounter(fmt.Sprintf("suite %q failures", suite.Name), suite.Failures, failures); err != nil {
			return err
		}
		if err := validateCounter(fmt.Sprintf("suite %q skipped", suite.Name), suite.Skipped, skipped); err != nil {
			return err
		}
		rootTests += suite.Tests
		rootErrors += suite.Errors
		rootFailures += suite.Failures
		rootSkipped += suite.Skipped
		rootDisabled += suite.Disabled
	}
	for _, counter := range []struct {
		name string
		got  int
		want int
	}{
		{"root tests", testsuites.Tests, rootTests},
		{"root errors", testsuites.Errors, rootErrors},
		{"root failures", testsuites.Failures, rootFailures},
		{"root skipped", testsuites.Skipped, rootSkipped},
		{"root disabled", testsuites.Disabled, rootDisabled},
	} {
		if err := validateCounter(counter.name, counter.got, counter.want); err != nil {
			return err
		}
	}
	return nil
}

func validateCounter(name string, got, want int) error {
	if got != want {
		return fmt.Errorf("%s counter is %d, want %d", name, got, want)
	}
	return nil
}

// Write atomically replaces a JUnit XML file.
func Write(path string, testsuites *Testsuites) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary JUnit report file for %q: %w", path, err)
	}
	tempPath := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tempPath)
	}()
	if err := f.Chmod(0o644); err != nil {
		return fmt.Errorf("failed to set permissions on temporary JUnit report file for %q: %w", path, err)
	}

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err := encoder.Encode(testsuites); err != nil {
		return fmt.Errorf("failed to encode JUnit report file %q: %w", path, err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to finish JUnit report file %q: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync JUnit report file %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close JUnit report file %q: %w", path, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("failed to replace JUnit report file %q: %w", path, err)
	}
	return nil
}
