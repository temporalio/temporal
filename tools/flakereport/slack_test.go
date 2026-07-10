package flakereport

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildFailureMessageIncludesFailedFinalTests(t *testing.T) {
	msg := buildFailureMessage("123", "main", "abcdef123456", "temporalio/temporal", []string{
		"TestOne",
		"TestTwo",
		"TestThree",
		"TestFour",
		"TestFive",
		"TestSix",
	})

	markdown := msg.renderMarkdown()
	require.Contains(t, markdown, "*Tests that failed final retry (up to 5):*")
	require.Contains(t, markdown, "TestOne")
	require.Contains(t, markdown, "TestFive")
	require.NotContains(t, markdown, "TestSix")
	require.Equal(t, 5, strings.Count(markdown, "•"))
}

func TestFailedFinalTestNames(t *testing.T) {
	tests := failedFinalTestNames([]TestFailure{
		{Name: "TestZeta (retry 2) (final)"},
		{Name: "TestAlpha (retry 1)"},
		{Name: "TestBeta (final)"},
		{Name: "TestAlpha (retry 2) (final)"},
		{Name: "TestAlpha (retry 3) (final)"},
		{Name: "TestGamma (final)"},
		{Name: "TestDelta (final)"},
		{Name: "TestEpsilon (final)"},
		{Name: "TestEta (final)"},
	})

	require.Equal(t, []string{
		"TestAlpha",
		"TestBeta",
		"TestDelta",
		"TestEpsilon",
		"TestEta",
	}, tests)
}
