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
