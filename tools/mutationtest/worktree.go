package mutationtest

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

func resolveMutationFiles(repoRoot string, includePatterns string, excludePatterns string) ([]string, error) {
	includedFiles, err := resolveFiles(repoRoot, includePatterns, false)
	if err != nil {
		return nil, err
	}
	excludedFiles, err := resolveFiles(repoRoot, excludePatterns, false)
	if err != nil {
		return nil, err
	}
	excludedSet := make(map[string]struct{}, len(excludedFiles))
	for _, excludedFile := range excludedFiles {
		excludedSet[excludedFile] = struct{}{}
	}
	result := make([]string, 0, len(includedFiles))
	for _, includedFile := range includedFiles {
		if _, ok := excludedSet[includedFile]; ok {
			continue
		}
		result = append(result, includedFile)
	}
	return result, nil
}

func resolveFiles(repoRoot string, patterns string, requireTests bool) ([]string, error) {
	selectedFiles := make(map[string]struct{})
	for pattern := range strings.FieldsSeq(patterns) {
		candidates, err := candidatePatterns(repoRoot, pattern, requireTests)
		if err != nil {
			return nil, err
		}
		for _, candidate := range candidates {
			if err := addResolvedFiles(repoRoot, candidate, requireTests, selectedFiles); err != nil {
				return nil, err
			}
		}
	}
	result := make([]string, 0, len(selectedFiles))
	for selectedFile := range selectedFiles {
		result = append(result, selectedFile)
	}
	slices.Sort(result)
	return result, nil
}

func addResolvedFiles(repoRoot string, pattern string, requireTests bool, selectedFiles map[string]struct{}) error {
	matches, err := resolvePattern(repoRoot, pattern)
	if err != nil {
		return err
	}
	for _, match := range matches {
		if !isSelectableFile(match, requireTests) {
			continue
		}
		relPath, err := filepath.Rel(repoRoot, match)
		if err != nil {
			return err
		}
		selectedFiles[filepath.ToSlash(relPath)] = struct{}{}
	}
	return nil
}

func isSelectableFile(path string, requireTests bool) bool {
	if requireTests {
		return strings.HasSuffix(path, "_test.go")
	}
	return strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")
}

func resolvePattern(repoRoot string, pattern string) ([]string, error) {
	if hasGlob(pattern) {
		return filepath.Glob(filepath.Join(repoRoot, pattern))
	}
	absPath := filepath.Join(repoRoot, pattern)
	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			if strings.Contains(pattern, "/") || strings.Contains(pattern, string(filepath.Separator)) {
				return nil, nil
			}
			return resolveBasename(repoRoot, pattern)
		}
		return nil, err
	}
	if !info.IsDir() {
		return []string{absPath}, nil
	}
	matches := make([]string, 0)
	err = filepath.WalkDir(absPath, func(current string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		matches = append(matches, current)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func resolveBasename(repoRoot string, name string) ([]string, error) {
	matches := make([]string, 0)
	err := filepath.WalkDir(repoRoot, func(current string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || d.Name() != name {
			return nil
		}
		matches = append(matches, current)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func candidatePatterns(repoRoot string, pattern string, requireTests bool) ([]string, error) {
	candidates := []string{pattern}
	if !requireTests {
		return candidates, nil
	}
	if strings.HasSuffix(pattern, ".go") && !strings.HasSuffix(pattern, "_test.go") {
		candidates = append(candidates, strings.TrimSuffix(pattern, ".go")+"_test.go")
	}
	if hasGlob(pattern) || filepath.Ext(pattern) != "" {
		return candidates, nil
	}
	info, err := os.Stat(filepath.Join(repoRoot, pattern))
	if err != nil {
		if os.IsNotExist(err) {
			return candidates, nil
		}
		return nil, err
	}
	if info.IsDir() {
		return candidates, nil
	}
	return candidates, nil
}

func hasGlob(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[")
}

func prepareTestFiles(worktreeDir string, selectedTestFiles []string) error {
	selectedFiles := make(map[string]struct{}, len(selectedTestFiles))
	for _, selectedTestFile := range selectedTestFiles {
		selectedFiles[filepath.Join(worktreeDir, filepath.FromSlash(selectedTestFile))] = struct{}{}
	}
	return filepath.WalkDir(worktreeDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if _, ok := selectedFiles[path]; ok {
			return nil
		}
		return os.Rename(path, path+".excluded")
	})
}

func prepareRunDir(ctx context.Context, outputRoot string) (string, error) {
	runDir := outputRoot
	if err := validateRunDir(runDir); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(runDir)
	if err == nil {
		for _, entry := range entries {
			if err := removeMutationOutputEntry(ctx, runDir, entry); err != nil {
				return "", err
			}
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", err
	}
	return runDir, nil
}

func validateRunDir(runDir string) error {
	_, err := os.Stat(filepath.Join(runDir, ".git"))
	if err == nil {
		return fmt.Errorf("output root must not be a git repository root: %s", runDir)
	}
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func removeMutationOutputEntry(ctx context.Context, runDir string, entry os.DirEntry) error {
	if entry.IsDir() {
		if !isNumberedArtifact(entry.Name(), "worktree-", "") {
			return nil
		}
		worktreeDir := filepath.Join(runDir, entry.Name())
		if err := gitWorktreeRemove(context.WithoutCancel(ctx), worktreeDir); err == nil {
			return nil
		}
		return os.RemoveAll(worktreeDir)
	}
	if isMutationOutputFile(entry.Name()) {
		return os.Remove(filepath.Join(runDir, entry.Name()))
	}
	return nil
}

func isMutationOutputFile(name string) bool {
	switch name {
	case "source-files.txt", "test-files.txt", "summary.txt", "survivors.diff", "uncovered.txt", "coverage.out":
		return true
	default:
		return isNumberedArtifact(name, "shard-", ".log") ||
			isNumberedArtifact(name, "survivors-", ".diff")
	}
}

func isNumberedArtifact(name string, prefix string, suffix string) bool {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	value := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if value == "" {
		return false
	}
	number, err := strconv.Atoi(value)
	return err == nil && number > 0
}

func writeLines(path string, lines []string) error {
	var buf bytes.Buffer
	for _, line := range lines {
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}
