package optimizetestsharding

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/github"
)

func TestLoadTestDataUsesRuntimeShardingUnits(t *testing.T) {
	dir := t.TempDir()
	writeJUnitReport(t, filepath.Join(dir, "run-1", "results.xml"), `<testsuites>
	<testsuite name="functional">
		<testcase name="TestSuite" classname="go.temporal.io/server/tests" time="25.0"></testcase>
		<testcase name="TestSuite/TestFlaky" classname="go.temporal.io/server/tests" time="4.0"></testcase>
		<testcase name="TestSuite/TestFlaky (retry 1) (final)" classname="go.temporal.io/server/tests" time="7.0"></testcase>
		<testcase name="TestSuite/TestSkipped" classname="go.temporal.io/server/tests" time="30.0"><skipped/></testcase>
		<testcase name="TestSuite/TestDeep" classname="go.temporal.io/server/tests" time="11.0"></testcase>
		<testcase name="TestSuite/TestDeep/case-a" classname="go.temporal.io/server/tests" time="2.0"></testcase>
		<testcase name="TestSuite/TestDeep/case-b" classname="go.temporal.io/server/tests" time="3.0"></testcase>
		<testcase name="TestSuite/TestTimeout (total timeout)" classname="go.temporal.io/server/tests" time="6.0"></testcase>
		<testcase name="TestStandalone" classname="go.temporal.io/server/tests" time="8.0"></testcase>
	</testsuite>
</testsuites>`)

	got, err := loadTestData(dir)
	require.NoError(t, err)
	require.Equal(t, map[string][]float64{
		"TestStandalone":                        {8},
		"TestSuite/TestDeep":                    {5},
		"TestSuite/TestFlaky":                   {7},
		"TestSuite/TestTimeout (total timeout)": {6},
	}, got)
}

func TestAggregateRunsUsesMedianRunTime(t *testing.T) {
	got := aggregateRuns(map[string][]float64{
		"TestStandalone":    {7, 9, 8},
		"TestSuite/TestOne": {10, 12, 200, 11, 13},
		"TestSuite/TestTwo": {4, 6},
	})

	require.Equal(t, map[string]float64{
		"TestStandalone":    8,
		"TestSuite/TestOne": 12,
		"TestSuite/TestTwo": 5,
	}, got)
}

func TestLoadTestDataCombinesArtifactsWithinRun(t *testing.T) {
	dir := t.TempDir()
	writeJUnitReport(t, filepath.Join(dir, "run-1", "database-job-1", "results.xml"), `<testsuite name="functional">
	<testcase name="TestSuite/TestOne" classname="go.temporal.io/server/tests" time="4.0"></testcase>
</testsuite>`)
	writeJUnitReport(t, filepath.Join(dir, "run-1", "database-job-2", "results.xml"), `<testsuite name="functional">
	<testcase name="TestSuite/TestOne" classname="go.temporal.io/server/tests" time="6.0"></testcase>
</testsuite>`)

	got, err := loadTestData(dir)
	require.NoError(t, err)
	require.Equal(t, map[string][]float64{"TestSuite/TestOne": {10}}, got)
}

func TestLoadTestDataUsesLatestWorkflowRunAttempt(t *testing.T) {
	dir := t.TempDir()
	writeJUnitReport(t,
		filepath.Join(dir, "run-1", "junit-xml--1--11--1--mysql8--shard0--functional-test", "results.xml"),
		`<testsuite name="functional">
	<testcase name="TestSuite/TestOne" classname="go.temporal.io/server/tests" time="100.0"></testcase>
</testsuite>`)
	writeJUnitReport(t,
		filepath.Join(dir, "run-1", "junit-xml--1--12--2--mysql8--shard0--functional-test", "results.xml"),
		`<testsuite name="functional">
	<testcase name="TestSuite/TestOne" classname="go.temporal.io/server/tests" time="5.0"></testcase>
</testsuite>`)

	got, err := loadTestData(dir)
	require.NoError(t, err)
	require.Equal(t, map[string][]float64{"TestSuite/TestOne": {5}}, got)
}

func TestLoadTestDataRejectsRunWithoutShardingUnits(t *testing.T) {
	dir := t.TempDir()
	writeJUnitReport(t, filepath.Join(dir, "run-1", "results.xml"), `<testsuites>
	<testsuite name="functional">
		<testcase name="TestSuite/TestSkipped" classname="go.temporal.io/server/tests" time="0"><skipped/></testcase>
	</testsuite>
</testsuites>`)

	_, err := loadTestData(dir)
	require.ErrorContains(t, err, "no test sharding units")
}

func TestOptimizeShardingSaltBreaksMaxTimeTiesBySpread(t *testing.T) {
	weights := map[string]float64{
		"TestSuite/TestA": 8,
		"TestSuite/TestB": 5,
		"TestSuite/TestC": 3,
		"TestSuite/TestD": 2,
	}

	got, _ := optimizeShardingSalt(weights, 3, 100)
	require.Equal(t, "-salt-33", got)
}

func TestDownloadRunArtifactsRetriesAndCleansPartialDownload(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "run-1")
	attempts := 0
	downloader := func(_ context.Context, _ string, opts github.RunDownloadOptions) error {
		attempts++
		if attempts < 3 {
			require.NoError(t, os.MkdirAll(opts.Dir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(opts.Dir, "partial"), nil, 0o600))
			return errors.New("transient download failure")
		}
		_, err := os.Stat(filepath.Join(opts.Dir, "partial"))
		require.ErrorIs(t, err, os.ErrNotExist)
		return nil
	}

	err := downloadRunArtifacts(context.Background(), "1", "junit-*", runDir, time.Nanosecond, downloader)
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestDownloadRunArtifactsFailsAfterThreeAttempts(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "run-1")
	attempts := 0
	downloader := func(context.Context, string, github.RunDownloadOptions) error {
		attempts++
		return errors.New("download failure")
	}

	err := downloadRunArtifacts(context.Background(), "1", "junit-*", runDir, time.Nanosecond, downloader)
	require.ErrorContains(t, err, "after 3 attempts")
	require.Equal(t, 3, attempts)
}

func writeJUnitReport(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
}
