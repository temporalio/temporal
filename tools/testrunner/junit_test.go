package testrunner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeXML(t *testing.T) {
	require.Equal(t, "valid\t\n\rtext", sanitizeXML("valid\x00\t\n\rtext\ufffe"))
}

func TestTruncateDetailsPreservesUTF8(t *testing.T) {
	details := strings.Repeat("é", junitDetailsMaxBytes)
	truncated := truncateDetails(details)
	require.Contains(t, truncated, "... (truncated) ...")
	require.Equal(t, strings.ToValidUTF8(truncated, ""), truncated)
	require.LessOrEqual(t, len(truncated), junitDetailsMaxBytes)
}
