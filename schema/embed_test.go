package schema

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaDirs(t *testing.T) {
	dirs := PathsByDir("cassandra")
	requireContains(t, []string{
		"cassandra/temporal",
	}, dirs)

	dirs = PathsByDir("mysql")
	requireContains(t, []string{
		"mysql/v8/temporal",
		"mysql/v8/visibility",
	}, dirs)

	dirs = PathsByDir("postgresql")
	requireContains(t, []string{
		"postgresql/v12/temporal",
		"postgresql/v12/visibility",
	}, dirs)
}

func TestElasticsearchIndexTemplateIsLatest(t *testing.T) {
	embeddedContent, err := ElasticsearchIndexTemplate()
	require.NoError(t, err, "Failed to get embedded index template")

	symlinkPath := "elasticsearch/visibility/index_template_v7.json"
	symlinkInfo, err := os.Lstat(symlinkPath)
	require.NoError(t, err, "Failed to get symlink info")
	require.True(t, symlinkInfo.Mode()&os.ModeSymlink != 0, "File is not a symlink")

	targetPath, err := os.Readlink(symlinkPath)
	require.NoError(t, err, "Failed to read symlink target")

	fullTargetPath := filepath.Join(filepath.Dir(symlinkPath), targetPath)
	targetContent, err := os.ReadFile(fullTargetPath)
	require.NoError(t, err, "Failed to read symlink target file")

	require.Equal(t, string(targetContent), embeddedContent,
		"Embedded content does not match symlink target. "+
			"Symlink points to: %s. "+
			"Update the ElasticsearchIndexTemplate() function to use the correct path.", targetPath)
}

func requireContains(t *testing.T, expected []string, actual []string) {
	for _, v := range expected {
		require.Contains(t, actual, v)
	}
}
