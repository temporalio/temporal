package junit

import (
	"encoding/xml"
	"fmt"
	"os"

	junitxml "github.com/jstemmer/go-junit-report/v2/junit"
)

// Read reads a JUnit XML file with either a testsuites or testsuite root.
func Read(path string) (*junitxml.Testsuites, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open JUnit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	decoder := xml.NewDecoder(f)
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("failed to read JUnit report file: %w", err)
		}
		root, ok := token.(xml.StartElement)
		if !ok {
			continue
		}

		switch root.Name.Local {
		case "testsuites":
			var testsuites junitxml.Testsuites
			if err := decoder.DecodeElement(&testsuites, &root); err != nil {
				return nil, fmt.Errorf("failed to read JUnit report file: %w", err)
			}
			return &testsuites, nil
		case "testsuite":
			var testsuite junitxml.Testsuite
			if err := decoder.DecodeElement(&testsuite, &root); err != nil {
				return nil, fmt.Errorf("failed to read JUnit report file: %w", err)
			}
			return &junitxml.Testsuites{
				Tests:    testsuite.Tests,
				Errors:   testsuite.Errors,
				Failures: testsuite.Failures,
				Time:     testsuite.Time,
				Suites:   []junitxml.Testsuite{testsuite},
			}, nil
		default:
			return nil, fmt.Errorf("failed to read JUnit report file: unexpected root element %q", root.Name.Local)
		}
	}
}

// Write writes a JUnit XML file.
func Write(path string, testsuites *junitxml.Testsuites) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to open JUnit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err := encoder.Encode(testsuites); err != nil {
		return fmt.Errorf("failed to write JUnit report file: %w", err)
	}
	return nil
}
