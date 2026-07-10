package cinotify

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailures(t *testing.T) {
	suites, err := parseJUnit([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="suite">
    <testcase classname="history" name="TestHistoryWorkflow (retry 1) (final)">
      <failure message="failed">failed</failure>
    </testcase>
    <testcase classname="history" name="TestRetryFailure (retry 1)">
      <failure message="failed">failed</failure>
    </testcase>
    <testcase classname="history" name="TestSkippedFinal (final)">
      <skipped message="skipped"/>
      <failure message="failed">failed</failure>
    </testcase>
    <testcase classname="history" name="TestPassedFinal (final)"/>
  </testsuite>
  <testsuite name="DATA RACE">
    <testcase classname="race" name="DATA RACE: detected">
      <failure message="WARNING: DATA RACE">race details</failure>
    </testcase>
  </testsuite>
</testsuites>`))
	require.NoError(t, err)

	require.Equal(t, []string{"TestHistoryWorkflow", "DATA RACE: detected"}, failures(suites))
}

func TestParseJUnitSingleTestsuite(t *testing.T) {
	suites, err := parseJUnit([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="suite">
  <testcase classname="matching" name="TestMatchingWorkflow (final)">
    <failure message="failed">failed</failure>
  </testcase>
</testsuite>`))
	require.NoError(t, err)

	require.Equal(t, []string{"TestMatchingWorkflow"}, failures(suites))
}
