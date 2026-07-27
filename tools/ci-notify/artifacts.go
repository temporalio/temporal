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

const (
	temporalRepository    = "temporalio/temporal"
	summaryArtifactPrefix = "test-summary-json--"
	summaryFileSuffix     = "test-summary.json"
	summaryKindOOM        = "OOM"
	summaryKindDataRace   = "DATA RACE"
)

var trailingFailureSuffixRegex = regexp.MustCompile(`\s*\([^)]+\)$`)

type testSummary struct {
	Rows []summaryRow `json:"rows"`
}

type summaryRow struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Details string `json:"details"`
	Final   bool   `json:"final"`
}

// forEachSummaryZip downloads every test-summary artifact for the run and
// invokes fn with the local path of each downloaded zip. Artifacts that fail to
// download are skipped. All downloads share a single temp dir that is removed
// when the function returns.
func forEachSummaryZip(ctx context.Context, runID int64, fn func(zipPath string)) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	artifacts, err := github.ListRunArtifacts(ctx, temporalRepository, runID)
	if err != nil {
		return err
	}

	tempDir, err := os.MkdirTemp("", "ci-notify-artifacts-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	for _, artifact := range artifacts {
		if artifact.Expired || !strings.HasPrefix(artifact.Name, summaryArtifactPrefix) {
			continue
		}

		zipPath, err := github.DownloadArtifact(ctx, temporalRepository, artifact.ID, tempDir)
		if err != nil {
			continue
		}
		fn(zipPath)
	}

	return nil
}

func getFailures(ctx context.Context, runID int64) ([]string, error) {
	var failures []string
	err := forEachSummaryZip(ctx, runID, func(zipPath string) {
		artifactFailures, err := failuresFromZip(zipPath)
		if err != nil {
			return
		}
		failures = append(failures, artifactFailures...)
	})
	if err != nil {
		return nil, err
	}
	return uniqueSorted(failures), nil
}

func failuresFromZip(zipPath string) ([]string, error) {
	rows, err := summaryRowsFromZip(zipPath)
	if err != nil {
		return nil, err
	}
	return reportableFailures(rows), nil
}

func summaryRowsFromZip(zipPath string) ([]summaryRow, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open artifact zip %s: %w", zipPath, err)
	}
	defer func() { _ = reader.Close() }()

	var rows []summaryRow
	for _, file := range reader.File {
		if file.FileInfo().IsDir() || !strings.HasSuffix(file.Name, summaryFileSuffix) {
			continue
		}

		fileRows, err := summaryRowsFromZipFile(file)
		if err != nil {
			continue
		}
		rows = append(rows, fileRows...)
	}
	return rows, nil
}

func summaryRowsFromZipFile(file *zip.File) ([]summaryRow, error) {
	rc, err := file.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open %s in artifact zip: %w", file.Name, err)
	}
	defer func() { _ = rc.Close() }()

	var summary testSummary
	if err := json.NewDecoder(rc).Decode(&summary); err != nil {
		return nil, fmt.Errorf("failed to parse %s in artifact zip: %w", file.Name, err)
	}
	return summary.Rows, nil
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
