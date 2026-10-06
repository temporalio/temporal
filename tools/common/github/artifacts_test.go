package github

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractArtifactFiles(t *testing.T) {
	zipPath := filepath.Join(t.TempDir(), "artifact.zip")
	zipFile, err := os.Create(zipPath)
	require.NoError(t, err)
	writer := zip.NewWriter(zipFile)
	for name, content := range map[string]string{
		"reports/one.xml": "one",
		"two.XML":         "two",
		"ignored.txt":     "ignored",
	} {
		file, err := writer.Create(name)
		require.NoError(t, err)
		_, err = file.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	require.NoError(t, zipFile.Close())

	outputDir := t.TempDir()
	paths, err := ExtractArtifactFiles(zipPath, outputDir, func(name string) bool {
		return strings.EqualFold(filepath.Ext(name), ".xml")
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		filepath.Join(outputDir, "one.xml"),
		filepath.Join(outputDir, "two.XML"),
	}, paths)
	one, err := os.ReadFile(filepath.Join(outputDir, "one.xml"))
	require.NoError(t, err)
	require.Equal(t, "one", string(one))
	_, err = os.Stat(filepath.Join(outputDir, "ignored.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

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
