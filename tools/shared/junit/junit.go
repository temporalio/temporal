package junit

import (
	"archive/zip"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	reportjunit "github.com/jstemmer/go-junit-report/v2/junit"
	sharedgithub "go.temporal.io/server/tools/shared/github"
)

// ParseFile reads a JUnit XML file, supporting both <testsuites> and a single <testsuite> root.
func ParseFile(path string) (*reportjunit.Testsuites, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open junit file %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()

	var suites reportjunit.Testsuites
	decoder := xml.NewDecoder(file)
	if err := decoder.Decode(&suites); err == nil {
		return &suites, nil
	}

	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek junit file %s: %w", path, err)
	}

	var suite reportjunit.Testsuite
	decoder = xml.NewDecoder(file)
	if err := decoder.Decode(&suite); err != nil {
		return nil, fmt.Errorf("failed to parse junit file %s: %w", path, err)
	}

	suites.Suites = []reportjunit.Testsuite{suite}
	return &suites, nil
}

func WalkTestcases(suites *reportjunit.Testsuites, fn func(reportjunit.Testcase)) {
	for _, suite := range suites.Suites {
		for _, testcase := range suite.Testcases {
			fn(testcase)
		}
	}
}

func CollectFailedTestNames(suites *reportjunit.Testsuites) []string {
	var names []string
	seen := make(map[string]struct{})

	WalkTestcases(suites, func(testcase reportjunit.Testcase) {
		if testcase.Failure == nil || testcase.Skipped != nil || testcase.Name == "" {
			return
		}
		if _, ok := seen[testcase.Name]; ok {
			return
		}
		seen[testcase.Name] = struct{}{}
		names = append(names, testcase.Name)
	})

	return names
}

func ExtractXMLFiles(zipPath, outputDir string) ([]string, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open zip file %s: %w", zipPath, err)
	}
	defer func() {
		_ = reader.Close()
	}()

	var xmlFiles []string
	for _, file := range reader.File {
		if file.FileInfo().IsDir() || !strings.HasSuffix(strings.ToLower(file.Name), ".xml") {
			continue
		}

		targetPath := filepath.Join(outputDir, filepath.Base(file.Name))
		rc, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open artifact entry %s: %w", file.Name, err)
		}
		out, err := os.Create(targetPath)
		if err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("failed to create extracted file %s: %w", targetPath, err)
		}
		_, copyErr := io.Copy(out, rc)
		closeErr := rc.Close()
		writeCloseErr := out.Close()
		if copyErr != nil {
			return nil, fmt.Errorf("failed to extract artifact entry %s: %w", file.Name, copyErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("failed to close artifact entry %s: %w", file.Name, closeErr)
		}
		if writeCloseErr != nil {
			return nil, fmt.Errorf("failed to close extracted file %s: %w", targetPath, writeCloseErr)
		}
		xmlFiles = append(xmlFiles, targetPath)
	}

	return xmlFiles, nil
}

func DownloadAndExtractXMLFiles(ctx context.Context, repo string, artifactID int64, outputDir string) ([]string, error) {
	zipPath, err := sharedgithub.DownloadArtifact(ctx, repo, artifactID, outputDir)
	if err != nil {
		return nil, err
	}
	return ExtractXMLFiles(zipPath, outputDir)
}
