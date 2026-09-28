package github

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseArtifactName(t *testing.T) {
	parsed, ok := ParseArtifactName("junit-xml--22373551837--64609560060--2--integration-0--Integration--functional-test")
	require.True(t, ok)
	require.Equal(t, ArtifactName{
		Type:       "junit-xml",
		RunID:      "22373551837",
		JobID:      "64609560060",
		RunAttempt: 2,
		NameSuffix: "integration-0--Integration--functional-test",
	}, parsed)

	parsed, ok = ParseArtifactName("junit-xml--1--2--invalid--mysql8--shard0--functional-test")
	require.False(t, ok)
	require.Equal(t, ArtifactName{
		Type:       "junit-xml",
		RunID:      "1",
		JobID:      "2",
		NameSuffix: "mysql8--shard0--functional-test",
	}, parsed)

	_, ok = ParseArtifactName("test-results")
	require.False(t, ok)
}
