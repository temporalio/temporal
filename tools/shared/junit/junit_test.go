package junit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFileSupportsTestsuitesAndTestsuite(t *testing.T) {
	t.Run("testsuites root", func(t *testing.T) {
		path := writeTempJUnitFile(t, `
<testsuites>
  <testsuite name="suite">
    <testcase name="TestOne"></testcase>
  </testsuite>
</testsuites>`)

		suites, err := ParseFile(path)
		require.NoError(t, err)
		require.Len(t, suites.Suites, 1)
		require.Equal(t, "TestOne", suites.Suites[0].Testcases[0].Name)
	})

	t.Run("single testsuite root", func(t *testing.T) {
		path := writeTempJUnitFile(t, `
<testsuite name="suite">
  <testcase name="TestOne"></testcase>
</testsuite>`)

		suites, err := ParseFile(path)
		require.NoError(t, err)
		require.Len(t, suites.Suites, 1)
		require.Equal(t, "TestOne", suites.Suites[0].Testcases[0].Name)
	})
}

func TestCollectFailedTestNames(t *testing.T) {
	path := writeTempJUnitFile(t, `
<testsuites>
  <testsuite name="suite">
    <testcase name="TestPass"></testcase>
    <testcase name="TestFailA"><failure message="boom"></failure></testcase>
    <testcase name="TestFailA"><failure message="boom again"></failure></testcase>
    <testcase name="TestSkipped"><failure message="skip"></failure><skipped></skipped></testcase>
    <testcase name="TestFailB"><failure message="boom"></failure></testcase>
  </testsuite>
</testsuites>`)

	suites, err := ParseFile(path)
	require.NoError(t, err)

	require.Equal(t, []string{"TestFailA", "TestFailB"}, CollectFailedTestNames(suites))
}

func writeTempJUnitFile(t *testing.T, contents string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "report.xml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0644))
	return path
}
