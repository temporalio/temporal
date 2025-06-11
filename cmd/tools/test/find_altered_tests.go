package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// find_altered_tests.go
//
// This program identifies altered or added test suite names based on the provided categories.
// It accepts the following inputs via command-line flags:
//
// - Category (-c): The category of tests to find (e.g., unit, integration, functional, functional_ndc).
// - Source Git Reference (-s): The source Git reference (commit SHA, branch, etc.).
// - Target Git Reference (-t): The target Git reference (commit SHA, branch, etc.).
//
// The program outputs environment variable assignments in the format `key=value`,
// where each key corresponds to a modified test suite category, and the value is a
// pipe-separated list of altered test suites.
//
// Usage:
//   go run find_altered_tests.go -c <category1> -c <category2> ... -s <sourceRef> -t <targetRef>

// CategoryDirs maps test categories to their corresponding directories
// If you update these, please also update the corresponding dirs in the Makefile
// FUNCTIONAL_TEST_ROOT
// FUNCTIONAL_TEST_XDC_ROOT
// FUNCTIONAL_TEST_NDC_ROOT
// INTEGRATION_TEST_DIRS
// UNIT_TEST_DIRS
var CategoryDirs = map[string][]string{
	"unit":           {"./client", "./common", "./internal", "./service", "./temporal", "./tools", "./cmd"},
	"integration":    {"./common/persistence/tests", "./tools/tests", "./temporaltest"},
	"functional":     {"./tests"},
	"functional_ndc": {"./tests/ndc"},
	"functional_xdc": {"./tests/xdc"},
}

func main() {
	var categories multiFlag
	var sourceRef string
	var targetRef string

	flag.Var(&categories, "c", "Category of altered tests to find (can specify multiple)")
	flag.StringVar(&sourceRef, "s", "", "Source Git reference (commit SHA, branch, etc.)")
	flag.StringVar(&targetRef, "t", "", "Target Git reference (commit SHA, branch, etc.)")
	flag.Parse()

	if len(categories) == 0 || sourceRef == "" || targetRef == "" {
		log.Fatalf("Usage: find_altered_tests -c <category1> -c <category2> ... -s <sourceRef> -t <targetRef>")
	}

	uniqCategories := make(map[string]struct{})
	for _, category := range categories {
		if _, exists := CategoryDirs[category]; !exists {
			log.Fatalf("Unknown category: %s", category)
		}
		uniqCategories[category] = struct{}{}
	}

	modifiedFiles, err := getModifiedTestFiles(sourceRef, targetRef)
	if err != nil {
		log.Fatalf("Error getting modified test files: %v", err)
	}

	for category := range uniqCategories {
		dirs := CategoryDirs[category]
		suites, err := findAlteredTestSuites(modifiedFiles, dirs)
		if err != nil {
			log.Fatalf("Error finding altered test suites for category %s: %v", category, err)
		}

		// Join suites and output the result directly
		joinedSuites := strings.Join(suites, "|")
		fmt.Printf("modified_%s_test_suites=%s\n", category, joinedSuites)
	}
}

// multiFlag allows multiple instances of a flag
type multiFlag []string

func (m *multiFlag) String() string {
	return strings.Join(*m, ",")
}

func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}

// getModifiedTestFiles runs 'git diff' to find modified '_test.go' files between two references
func getModifiedTestFiles(sourceRef, targetRef string) ([]string, error) {
	cmd := exec.Command("git", "diff", "--name-only", sourceRef, targetRef)
	output, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("git diff error: %w", exitErr)
		}
		return nil, fmt.Errorf("error running git diff: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var testFiles []string
	for _, line := range lines {
		if strings.HasSuffix(line, "_test.go") {
			testFiles = append(testFiles, line)
		}
	}
	return testFiles, nil
}

// findAlteredTestSuites filters files by the test directories and extracts test suite names from them
func findAlteredTestSuites(files []string, testDirs []string) ([]string, error) {
	var testSuites []string
	suiteSet := make(map[string]struct{})

	// to be detected, _test.go file must contain TestXXXXSuite function
	testSuiteRegex := regexp.MustCompile(`func\s+(Test[a-zA-Z0-9_]*Suite)\s*\(`)

	for _, file := range files {
		if _, err := os.Stat(file); err != nil {
			continue
		}

		filePath := filepath.Clean(file)

		if !isInAnyPath(filePath, testDirs) {
			continue
		}

		content, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}

		// Find test suite names in the file
		matches := testSuiteRegex.FindAllStringSubmatch(string(content), -1)
		for _, match := range matches {
			suiteName := match[1]
			if _, exists := suiteSet[suiteName]; !exists {
				suiteSet[suiteName] = struct{}{}
				testSuites = append(testSuites, suiteName)
			}
		}
	}

	return testSuites, nil
}

// isInAnyPath checks if the file is within any of the specified paths
func isInAnyPath(file string, paths []string) bool {
	for _, path := range paths {
		cleanPath := filepath.Clean(path)
		if !strings.HasSuffix(cleanPath, string(os.PathSeparator)) {
			cleanPath += string(os.PathSeparator)
		}
		if strings.HasPrefix(file, cleanPath) {
			return true
		}
	}
	return false
}
