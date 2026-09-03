package mutationtest

import (
	"fmt"
	"path/filepath"
)

type mutationRunStats struct {
	killed     int
	survived   int
	skipped    int
	duplicates int
	uncovered  int
}

func aggregateStats(results []shardResult, duplicates int, uncovered int) mutationRunStats {
	stats := mutationRunStats{duplicates: duplicates, uncovered: uncovered}
	for _, result := range results {
		stats.killed += result.killed
		stats.survived += result.survived
		stats.skipped += result.skipped
	}
	return stats
}

func writeMutationSummary(repoRoot string, runDir string, results []shardResult, stats mutationRunStats) error {
	lines := []string{
		"Mutation Summary",
		"",
		"Overall",
		fmt.Sprintf("  killed:      %d", stats.killed),
		fmt.Sprintf("  survived:    %d", stats.survived),
		fmt.Sprintf("  skipped:     %d", stats.skipped),
		fmt.Sprintf("  duplicated:  %d", stats.duplicates),
		fmt.Sprintf("  uncovered:   %d", stats.uncovered),
		fmt.Sprintf("  mutants:     %d", stats.killed+stats.survived+stats.skipped),
		"",
		"Logs",
	}
	for _, result := range results {
		lines = append(lines, "  "+displayPath(repoRoot, result.logPath))
	}
	lines = append(lines,
		"",
		"Surviving Diffs",
		"  "+displayPath(repoRoot, filepath.Join(runDir, "survivors.diff")),
		"",
		"Uncovered Blocks",
		"  "+displayPath(repoRoot, filepath.Join(runDir, "uncovered.txt")),
	)
	for _, line := range lines {
		fmt.Println(line)
	}
	return writeLines(filepath.Join(runDir, "summary.txt"), lines)
}
