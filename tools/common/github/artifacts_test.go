package github

import (
	"archive/zip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestDownloadArtifactRetriesIncompleteResponses(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts < 3 {
			writer.Header().Set("Content-Length", "8")
			_, _ = writer.Write([]byte("bad"))
			return
		}
		_, _ = writer.Write([]byte("complete"))
	}))
	defer server.Close()

	restoreAPIClient(t, server.URL, server.Client())
	t.Setenv("GH_TOKEN", "test-token")
	outputDir := t.TempDir()

	zipPath, err := downloadArtifactWithRetry(
		context.Background(),
		"temporalio/temporal",
		42,
		outputDir,
		time.Nanosecond,
	)
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	content, err := os.ReadFile(zipPath)
	require.NoError(t, err)
	require.Equal(t, "complete", string(content))
	entries, err := os.ReadDir(outputDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Base(zipPath), entries[0].Name())
}

func TestDownloadRunArtifactsFiltersByPattern(t *testing.T) {
	var downloaded []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repos/temporalio/temporal/actions/runs/123/artifacts":
			_, _ = writer.Write([]byte(`{"artifacts":[
				{"id":1,"name":"junit-xml--one"},
				{"id":2,"name":"junit-xml--expired","expired":true},
				{"id":3,"name":"debug-logs--one"}
			]}`))
		case "/repos/temporalio/temporal/actions/artifacts/1/zip":
			downloaded = append(downloaded, request.URL.Path)
			_, _ = writer.Write([]byte("artifact"))
		default:
			http.Error(writer, fmt.Sprintf("unexpected request %s", request.URL.Path), http.StatusNotFound)
		}
	}))
	defer server.Close()

	restoreAPIClient(t, server.URL, server.Client())
	t.Setenv("GH_TOKEN", "test-token")
	outputDir := t.TempDir()

	downloads, err := DownloadRunArtifacts(
		context.Background(),
		"temporalio/temporal",
		123,
		"junit-*",
		outputDir,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"/repos/temporalio/temporal/actions/artifacts/1/zip"}, downloaded)
	require.Equal(t, []DownloadedArtifact{{
		Artifact: Artifact{ID: 1, Name: "junit-xml--one"},
		ZipPath:  filepath.Join(outputDir, "artifact-1.zip"),
	}}, downloads)
	content, err := os.ReadFile(downloads[0].ZipPath)
	require.NoError(t, err)
	require.Equal(t, "artifact", string(content))
}
