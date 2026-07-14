package cinotify

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
)

var trailingFailureSuffixRegex = regexp.MustCompile(`\s*\([^)]+\)$`)

const summaryKindOOM = "OOM"

type testSummary struct {
	Rows []summaryRow `json:"rows"`
}

type summaryRow struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Final bool   `json:"final"`
}

func getFailures(ctx context.Context, runID int64) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	artifacts, err := github.ListRunArtifacts(ctx, "temporalio/temporal", runID)
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
		if artifact.Expired || !strings.HasPrefix(artifact.Name, "test-summary-json--") {
			continue
		}

		zipPath, err := github.DownloadArtifact(ctx, "temporalio/temporal", artifact.ID, tempDir)
		if err != nil {
			continue
		}

		artifactFailures, err := failuresFromZip(zipPath)
		if err != nil {
			continue
		}
		failures = append(failures, artifactFailures...)
	}

	return uniqueSorted(failures), nil
}

func failuresFromZip(zipPath string) ([]string, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open artifact zip %s: %w", zipPath, err)
	}
	defer func() { _ = reader.Close() }()

	var failures []string
	for _, file := range reader.File {
		if file.FileInfo().IsDir() || !strings.HasSuffix(file.Name, "test-summary.json") {
			continue
		}

		fileFailures, err := failuresFromZipFile(file)
		if err != nil {
			continue
		}
		failures = append(failures, fileFailures...)
	}
	return failures, nil
}

func failuresFromZipFile(file *zip.File) ([]string, error) {
	rc, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open %s in artifact zip: %w", file.Name, err)
	}
	defer func() { _ = rc.Close() }()

	var summary testSummary
	if err := json.NewDecoder(rc).Decode(&summary); err != nil {
		return nil, fmt.Errorf("failed to parse %s in artifact zip: %w", file.Name, err)
	}
	return reportableFailures(summary.Rows), nil
}

func reportableFailures(rows []summaryRow) []string {
	var failures []string
	for _, row := range rows {
		if !row.Final && row.Kind != summaryKindOOM {
			continue
		}
		if row.Kind == summaryKindOOM {
			failures = append(failures, summaryKindOOM)
			continue
		}
		failures = append(failures, normalizeFailureName(row.Name))
	}
	return failures
}

func normalizeFailureName(name string) string {
	for {
		stripped := trailingFailureSuffixRegex.ReplaceAllString(name, "")
		if stripped == name {
			return name
		}
		name = stripped
	}
}

func uniqueSorted(values []string) []string {
	values = slices.Clone(values)
	slices.Sort(values)
	return slices.Compact(values)
}
