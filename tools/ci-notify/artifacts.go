package cinotify

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
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
	Final bool   `json:"final,omitempty"`
}

func getFailures(ctx context.Context, run github.Run, runID string) ([]string, error) {
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
		if artifact.Expired || !isSummaryArtifact(artifact.Name) {
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

func isSummaryArtifact(name string) bool {
	return strings.HasPrefix(name, "test-summary-json--")
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

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s in artifact zip: %w", file.Name, err)
	}

	var summary testSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return nil, fmt.Errorf("failed to parse %s in artifact zip: %w", file.Name, err)
	}
	return reportableFailures(summary.Rows), nil
}

func reportableFailures(rows []summaryRow) []string {
	var failures []string
	for _, row := range rows {
		if !isReportableFailure(row) {
			continue
		}
		failures = append(failures, failureName(row))
	}
	return failures
}

func isReportableFailure(row summaryRow) bool {
	return row.Final || row.Kind == summaryKindOOM
}

func failureName(row summaryRow) string {
	if row.Kind == summaryKindOOM {
		return summaryKindOOM
	}
	return normalizeFailureName(row.Name)
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
