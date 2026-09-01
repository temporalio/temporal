package mutationtest

import (
	"context"
	"fmt"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/tools/cover"
)

type uncoveredFinding struct {
	file        string
	startLine   int
	startColumn int
	endLine     int
	endColumn   int
	statements  int
}

type coverageIndex map[string][]cover.ProfileBlock

func collectCoverage(
	ctx context.Context,
	worktreeDir string,
	targets []targetFile,
	cfg config,
	profilePath string,
) (coverageIndex, []uncoveredFinding, error) {
	packageSet := make(map[string]struct{})
	for _, target := range targets {
		packageSet[target.packagePath] = struct{}{}
	}
	packagePaths := make([]string, 0, len(packageSet))
	for packagePath := range packageSet {
		packagePaths = append(packagePaths, packagePath)
	}
	slices.Sort(packagePaths)

	args := testCommandArgs(cfg)
	args = slices.Insert(args, len(args)-1,
		"-coverprofile", profilePath,
		"-coverpkg", strings.Join(packagePaths, ","),
	)
	cmd := newCommand(ctx, "go", args...)
	cmd.Dir = worktreeDir
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, nil, fmt.Errorf("coverage tests failed: %w\n%s", err, strings.TrimSpace(string(output)))
	}
	profiles, err := cover.ParseProfiles(profilePath)
	if err != nil {
		return nil, nil, fmt.Errorf("parse coverage profile: %w", err)
	}

	targetByCoveragePath := make(map[string]targetFile, len(targets))
	for _, target := range targets {
		targetByCoveragePath[filepath.ToSlash(target.coveragePath)] = target
	}
	coverage := make(coverageIndex, len(targets))
	for _, profile := range profiles {
		profilePath := filepath.ToSlash(profile.FileName)
		if _, ok := targetByCoveragePath[profilePath]; !ok {
			continue
		}
		coverage[profilePath] = append(coverage[profilePath], profile.Blocks...)
	}

	uncovered := make([]uncoveredFinding, 0)
	for _, target := range targets {
		blocks, ok := coverage[target.coveragePath]
		if !ok {
			return nil, nil, fmt.Errorf("coverage profile omitted mutation target %s", target.relativePath)
		}
		for _, block := range blocks {
			if block.Count != 0 {
				continue
			}
			uncovered = append(uncovered, uncoveredFinding{
				file:        target.relativePath,
				startLine:   block.StartLine,
				startColumn: block.StartCol,
				endLine:     block.EndLine,
				endColumn:   block.EndCol,
				statements:  block.NumStmt,
			})
		}
	}
	slices.SortFunc(uncovered, func(left, right uncoveredFinding) int {
		if comparison := strings.Compare(left.file, right.file); comparison != 0 {
			return comparison
		}
		if left.startLine != right.startLine {
			return left.startLine - right.startLine
		}
		return left.startColumn - right.startColumn
	})
	return coverage, uncovered, nil
}

func testCommandArgs(cfg config) []string {
	return []string{
		"test",
		"-count=1",
		"-timeout", cfg.timeout.String(),
		"-tags", cfg.testTags,
		"./...",
	}
}

func (coverage coverageIndex) covers(target targetFile, position token.Pos) bool {
	location := target.fileSet.Position(position)
	for _, block := range coverage[target.coveragePath] {
		if block.Count > 0 && positionWithinBlock(location.Line, location.Column, block) {
			return true
		}
	}
	return false
}

func positionWithinBlock(line int, column int, block cover.ProfileBlock) bool {
	if line < block.StartLine || line > block.EndLine {
		return false
	}
	if line == block.StartLine && column < block.StartCol {
		return false
	}
	return line != block.EndLine || column < block.EndCol
}

func writeUncoveredFindings(path string, findings []uncoveredFinding) error {
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, fmt.Sprintf(
			"%s:%d:%d-%d:%d statements=%d",
			finding.file,
			finding.startLine,
			finding.startColumn,
			finding.endLine,
			finding.endColumn,
			finding.statements,
		))
	}
	return writeLines(path, lines)
}
