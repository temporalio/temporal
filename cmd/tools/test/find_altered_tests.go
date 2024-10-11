package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// find_altered_tests.go
//
// This program identifies altered or added test suite names based on the provided criteria.
// It accepts the following inputs via command-line flags:
//
// - Category (-c): The category of tests to find (e.g., unit, integration).
// - Source Git Reference (-source-ref): The source Git reference (commit SHA, branch, etc.).
// - Target Git Reference (-target-ref): The target Git reference (commit SHA, branch, etc.).
// - Test Directories (-d): Comma-separated list of test directories to search.
//
// The program outputs the names of altered test suites, separated by '|'.
//
// Usage:
//   find_altered_tests -c <category> -source-ref <sourceRef> -target-ref <targetRef> -d <testDirs>

func main() {
	var category string
	var sourceRef string
	var targetRef string
	var testDirs string

	flag.StringVar(&category, "c", "", "Category of altered tests to find")
	flag.StringVar(&sourceRef, "source-ref", "", "Source Git reference (commit SHA, branch, etc.)")
	flag.StringVar(&targetRef, "target-ref", "", "Target Git reference (commit SHA, branch, etc.)")
	flag.StringVar(&testDirs, "d", "", "Comma-separated list of test directories")
	flag.Parse()

	if category == "" || sourceRef == "" || targetRef == "" || testDirs == "" {
		fmt.Println("Usage: find_altered_tests -c <category> -source-ref <sourceRef> -target-ref <targetRef> -d <testDirs>")
		os.Exit(1)
	}

	modifiedFiles, err := getModifiedTestFiles(sourceRef, targetRef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting modified test files: %v\n", err)
		os.Exit(1)
	}

	testSuites, err := findAlteredTestSuites(modifiedFiles, testDirs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error finding altered test suites: %v\n", err)
		os.Exit(1)
	}

	if len(testSuites) == 0 {
		// No modified test suites found
		os.Exit(0)
	}

	// Output the test suite names, separated by '|'
	fmt.Println(strings.Join(testSuites, "|"))
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
func findAlteredTestSuites(files []string, testDirs string) ([]string, error) {
	var testSuites []string
	suiteSet := make(map[string]struct{})

	testSuiteRegex := regexp.MustCompile(`func\s+(Test[a-zA-Z0-9_]*Suite)\s*\(`)

	dirs := strings.Split(testDirs, ",")
	for i, dir := range dirs {
		dirs[i] = filepath.Clean(dir)
		if !strings.HasSuffix(dirs[i], string(os.PathSeparator)) {
			dirs[i] += string(os.PathSeparator)
		}
	}

	for _, file := range files {
		if _, err := os.Stat(file); err != nil {
			continue
		}

		filePath := filepath.Clean(file)

		if !isInAnyPath(filePath, dirs) {
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
		if strings.HasPrefix(file, path) {
			return true
		}
	}
	return false
}
