package cinotify

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"go.temporal.io/server/tools/common/github"
)

var finalTestRegex = regexp.MustCompile(`\s*\(final\)$`)
var trailingTestSuffixRegex = regexp.MustCompile(`\s*\([^)]+\)$`)

func getFinalFailures(ctx context.Context, run github.Run, runID string) ([]string, error) {
	artifactRunID, err := artifactRunID(run, runID)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	artifacts, err := github.ListRunArtifacts(ctx, "temporalio/temporal", artifactRunID)
	if err != nil {
		return nil, err
	}

	tempDir, err := os.MkdirTemp("", "ci-notify-artifacts-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	var failures []string
	for _, artifact := range artifacts {
		if artifact.Expired || !isJUnitArtifact(artifact.Name) {
			continue
		}

		zipPath, err := github.DownloadArtifact(ctx, "temporalio/temporal", artifact.ID, tempDir)
		if err != nil {
			continue
		}

		artifactFailures, err := finalFailuresFromZip(zipPath)
		if err != nil {
			continue
		}
		failures = append(failures, artifactFailures...)
	}

	return uniqueSorted(failures), nil
}

func artifactRunID(run github.Run, runID string) (int64, error) {
	if run.DatabaseID != 0 {
		return run.DatabaseID, nil
	}
	id, err := strconv.ParseInt(runID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid workflow run ID %q: %w", runID, err)
	}
	return id, nil
}

func isJUnitArtifact(name string) bool {
	return strings.Contains(strings.ToLower(name), "junit")
}

func finalFailuresFromZip(zipPath string) ([]string, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open artifact zip %s: %w", zipPath, err)
	}
	defer func() { _ = reader.Close() }()

	var failures []string
	for _, file := range reader.File {
		if file.FileInfo().IsDir() || !strings.HasSuffix(strings.ToLower(file.Name), ".xml") {
			continue
		}

		fileFailures, err := finalFailuresFromZipFile(file)
		if err != nil {
			continue
		}
		failures = append(failures, fileFailures...)
	}
	return failures, nil
}

func finalFailuresFromZipFile(file *zip.File) ([]string, error) {
	rc, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open %s in artifact zip: %w", file.Name, err)
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s in artifact zip: %w", file.Name, err)
	}

	suites, err := parseJUnit(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s in artifact zip: %w", file.Name, err)
	}
	return finalFailures(suites), nil
}

func parseJUnit(data []byte) (*junit.Testsuites, error) {
	var suites junit.Testsuites
	if err := xml.NewDecoder(bytes.NewReader(data)).Decode(&suites); err == nil {
		return &suites, nil
	}

	var suite junit.Testsuite
	if err := xml.NewDecoder(bytes.NewReader(data)).Decode(&suite); err != nil {
		return nil, err
	}
	return &junit.Testsuites{Suites: []junit.Testsuite{suite}}, nil
}

func finalFailures(suites *junit.Testsuites) []string {
	var failures []string
	for _, suite := range suites.Suites {
		for _, testcase := range suite.Testcases {
			if testcase.Failure == nil || testcase.Skipped != nil || !finalTestRegex.MatchString(testcase.Name) {
				continue
			}
			failures = append(failures, normalizeTestName(testcase.Name))
		}
	}
	return failures
}

func normalizeTestName(name string) string {
	for {
		stripped := trailingTestSuffixRegex.ReplaceAllString(name, "")
		if stripped == name {
			return name
		}
		name = stripped
	}
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	var unique []string
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	sort.Strings(unique)
	return unique
}
