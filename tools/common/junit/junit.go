package junit

import (
	"encoding/xml"
	"errors"
	"fmt"
	"os"

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
