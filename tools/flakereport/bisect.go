package flakereport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	minBisectFailures = 5  // skip test if fewer than this many failures in window
	minBisectRuns     = 30 // skip test if ran fewer than this many times total
)

// logBeta returns log(Beta(a, b)) = lgamma(a) + lgamma(b) - lgamma(a+b).
func logBeta(a, b float64) float64 {
	la, _ := math.Lgamma(a)
	lb, _ := math.Lgamma(b)
	lab, _ := math.Lgamma(a + b)
	return la + lb - lab
}

// logBetaBinomial returns the log marginal likelihood of observing k failures
// in n trials with a Beta(alpha, beta) prior on the failure probability.
// With Beta(1,1) prior this is the log Beta-Binomial marginal likelihood.
func logBetaBinomial(n, k int, alpha, beta float64) float64 {
	return logBeta(float64(k)+alpha, float64(n-k)+beta) - logBeta(alpha, beta)
}

// logSumExpNormalize converts log-weights to probabilities using the log-sum-exp trick
// to avoid numerical underflow. Returns a slice of probabilities summing to 1.0.
func logSumExpNormalize(logWeights []float64) []float64 {
	if len(logWeights) == 0 {
		return nil
	}
	// Find max for numerical stability
	maxLog := logWeights[0]
	for _, lw := range logWeights[1:] {
		if lw > maxLog {
			maxLog = lw
		}
	}
	// Compute exp(lw - maxLog) and sum
	probs := make([]float64, len(logWeights))
	sum := 0.0
	for i, lw := range logWeights {
		probs[i] = math.Exp(lw - maxLog)
		sum += probs[i]
	}
	// Normalize
	for i := range probs {
		probs[i] /= sum
	}
	return probs
}

// buildObservations groups TestRun records for a single (normalized) test name by commit SHA
// and returns them sorted chronologically using the commit order slice.
// runToSHA maps workflow RunID → commit SHA.
// commitOrder lists SHAs from oldest to newest.
// Runs whose SHA is not in commitOrder are skipped (e.g. from force-pushes or unrelated branches).
func buildObservations(testName string, runs []TestRun, commitOrderSlice []string, runToSHA map[int64]string) []CommitObservation {
	// Build commit index map: SHA → index
	commitIdx := make(map[string]int, len(commitOrderSlice))
	for i, sha := range commitOrderSlice {
		commitIdx[sha] = i
	}

	// Aggregate pass/fail counts per commit SHA for this test
	type counts struct{ passes, fails int }
	bySHA := make(map[string]*counts)

	for _, run := range runs {
		if run.Skipped {
			continue
		}
		if normalizeTestName(run.Name) != testName {
			continue
		}
		sha, ok := runToSHA[run.RunID]
		if !ok || sha == "" {
			continue
		}
		if _, inOrder := commitIdx[sha]; !inOrder {
			continue
		}
		if bySHA[sha] == nil {
			bySHA[sha] = &counts{}
		}
		if run.Failed {
			bySHA[sha].fails++
		} else {
			bySHA[sha].passes++
		}
	}

	// Convert to slice and sort by commit index
	obs := make([]CommitObservation, 0, len(bySHA))
	for sha, c := range bySHA {
		obs = append(obs, CommitObservation{
			CommitSHA: sha,
			CommitIdx: commitIdx[sha],
			Prior:     1.0, // uniform prior; adjusted by heuristics if enabled
			Passes:    c.passes,
			Fails:     c.fails,
		})
	}
	sort.Slice(obs, func(i, j int) bool {
		return obs[i].CommitIdx < obs[j].CommitIdx
	})
	return obs
}

// runBisect computes posterior probability for each candidate culprit commit.
// obs must be sorted by CommitIdx ascending (chronological).
// The model: there is one transition commit c; before c failure prob is p_before,
// at/after c failure prob is p_after. Both have Beta(1,1) priors.
// Returns results sorted by Probability descending.
// Returns nil if there are fewer than 2 commits with observations (can't localize).
func runBisect(obs []CommitObservation) []BisectResult {
	if len(obs) < 2 {
		return nil
	}

	// Prefix sums of fails and passes in commit order
	N := len(obs)
	prefixFails := make([]int, N+1)
	prefixPasses := make([]int, N+1)
	for i, o := range obs {
		prefixFails[i+1] = prefixFails[i] + o.Fails
		prefixPasses[i+1] = prefixPasses[i] + o.Passes
	}
	totalFails := prefixFails[N]
	totalPasses := prefixPasses[N]
	totalObs := totalFails + totalPasses

	if totalObs == 0 {
		return nil
	}

	// Compute log-posterior for each candidate culprit commit
	logWeights := make([]float64, N)
	for i, o := range obs {
		kBefore := prefixFails[i]
		nBefore := prefixPasses[i] + prefixFails[i]
		kAfter := totalFails - kBefore
		nAfter := totalObs - nBefore

		logWeights[i] = math.Log(o.Prior) +
			logBetaBinomial(nBefore, kBefore, 1, 1) +
			logBetaBinomial(nAfter, kAfter, 1, 1)
	}

	probs := logSumExpNormalize(logWeights)

	results := make([]BisectResult, N)
	for i, o := range obs {
		results[i] = BisectResult{
			CommitSHA:     o.CommitSHA,
			CommitIdx:     o.CommitIdx,
			Probability:   probs[i],
			FailsBefore:   prefixFails[i],
			PassesBefore:  prefixPasses[i],
			FailsAfter:    totalFails - prefixFails[i],
			PassesAfter:   totalPasses - prefixPasses[i],
			CommitTitle:   o.CommitSHA, // placeholder; overwritten if heuristics fetch metadata
			HeuristicNote: "",
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Probability > results[j].Probability
	})
	return results
}

// commitPriorWeight returns a prior weight multiplier [0.0, 2.0] for a commit,
// based on its changed files relative to the test name. For now this only reduces
// the prior for commits that only touch test files or docs/config files.
// Weight < 1.0 makes the commit less likely to be the culprit.
// Weight > 1.0 makes it more likely.
func commitPriorWeight(meta CommitMeta, testName string) (weight float64, note string) {
	files := meta.Files
	if len(files) == 0 {
		return 1.0, ""
	}

	// Commits only touching non-code paths cannot affect test behavior
	if allFilesMatchPrefixes(files, []string{".github/", "docs/", "CODEOWNERS"}) ||
		allFilesMatchSuffixes(files, []string{".md", ".txt", ".yaml", ".yml"}) {
		return 0.05, "only touches docs/.github/config — deprioritized"
	}

	// Commits only touching test files for other packages → mild deprioritization
	if allFilesMatchSuffixes(files, []string{"_test.go"}) {
		return 0.3, "only touches test files"
	}

	return 1.0, ""
}

// allFilesMatchPrefixes returns true if every file in the list starts with one of the given prefixes.
func allFilesMatchPrefixes(files, prefixes []string) bool {
	for _, f := range files {
		matched := false
		for _, p := range prefixes {
			if strings.HasPrefix(f, p) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// allFilesMatchSuffixes returns true if every file in the list ends with one of the given suffixes.
func allFilesMatchSuffixes(files, suffixes []string) bool {
	for _, f := range files {
		matched := false
		for _, s := range suffixes {
			if strings.HasSuffix(f, s) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// anyFileMatchesPrefix returns true if any file starts with the given prefix.
func anyFileMatchesPrefix(files []string, prefix string) bool {
	for _, f := range files {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	return false
}

// runBisectForTest runs the full bisect pipeline for a single test name.
// It builds observations, optionally applies heuristics, runs the inference, and returns the report.
func runBisectForTest(ctx context.Context, cfg BisectConfig, testName string, allRuns []TestRun, commitOrderSlice []string, runToSHA map[int64]string) TestBisectReport {
	obs := buildObservations(testName, allRuns, commitOrderSlice, runToSHA)

	totalPasses, totalFails := 0, 0
	for _, o := range obs {
		totalPasses += o.Passes
		totalFails += o.Fails
	}
	totalObs := totalPasses + totalFails

	// Check signal thresholds
	if totalFails < cfg.MinFailures {
		return TestBisectReport{
			TestName: testName,
			Skipped:  true,
		}
	}
	if totalObs < cfg.MinRuns {
		return TestBisectReport{
			TestName: testName,
			Skipped:  true,
		}
	}

	// Fetch commit metadata and apply heuristics to adjust priors.
	// commitMetas maps SHA → metadata for use in annotating results
	commitMetas := make(map[string]CommitMeta)
	for i := range obs {
		meta, err := fetchCommitMeta(ctx, cfg.Repo, obs[i].CommitSHA)
		if err != nil {
			continue
		}
		weight, _ := commitPriorWeight(meta, testName)
		obs[i].Prior = weight
		commitMetas[obs[i].CommitSHA] = meta
	}

	results := runBisect(obs)
	if results == nil {
		return TestBisectReport{
			TestName: testName,
			Skipped:  true,
		}
	}

	// Annotate results with commit metadata
	for i := range results {
		if meta, ok := commitMetas[results[i].CommitSHA]; ok {
			results[i].CommitTitle = meta.Title
			results[i].CommitAuthor = meta.Author
			_, results[i].HeuristicNote = commitPriorWeight(meta, testName)
		}
	}

	// Filter suspects below the minimum probability threshold.
	if cfg.MinProbability > 0 {
		filtered := results[:0]
		for _, r := range results {
			if r.Probability >= cfg.MinProbability {
				filtered = append(filtered, r)
			}
		}
		results = filtered
	}

	if len(results) == 0 {
		return TestBisectReport{
			TestName: testName,
			TotalObs: totalObs,
			Skipped:  true,
		}
	}

	return TestBisectReport{
		TestName:    testName,
		TopSuspects: results,
		TotalObs:    totalObs,
	}
}

// selectTopFlakyTests selects the top-N flakiest tests that meet minimum signal thresholds.
// allRuns is the full set of test runs. Returns normalized test names sorted by failure rate desc.
func selectTopFlakyTests(allRuns []TestRun, cfg BisectConfig) []string {
	type testStats struct {
		fails int
		total int
	}
	stats := make(map[string]*testStats)
	for _, run := range allRuns {
		if run.Skipped {
			continue
		}
		name := normalizeTestName(run.Name)
		if stats[name] == nil {
			stats[name] = &testStats{}
		}
		stats[name].total++
		if run.Failed {
			stats[name].fails++
		}
	}

	type ranked struct {
		name  string
		rate  float64
		fails int
	}
	var candidates []ranked
	for name, s := range stats {
		if s.fails < cfg.MinFailures || s.total < cfg.MinRuns {
			continue
		}
		candidates = append(candidates, ranked{
			name:  name,
			rate:  float64(s.fails) / float64(s.total),
			fails: s.fails,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].rate != candidates[j].rate {
			return candidates[i].rate > candidates[j].rate
		}
		return candidates[i].fails > candidates[j].fails
	})

	n := len(candidates)
	if cfg.TopN > 0 && cfg.TopN < n {
		n = cfg.TopN
	}

	names := make([]string, n)
	for i := range names {
		names[i] = candidates[i].name
	}
	return names
}

// runBisectAnalysis is the top-level bisect orchestrator called from the generate command.
// It runs bisect for the top-N flakiest tests and returns their reports.
func runBisectAnalysis(ctx context.Context, cfg BisectConfig, allRuns []TestRun, workflowRuns []WorkflowRun) ([]TestBisectReport, error) {
	// Build runID → SHA map from workflow runs
	runToSHA := make(map[int64]string, len(workflowRuns))
	var oldestSHA string
	var oldestTime time.Time
	for _, wr := range workflowRuns {
		if wr.HeadSHA == "" {
			continue
		}
		runToSHA[wr.ID] = wr.HeadSHA
		if oldestTime.IsZero() || wr.CreatedAt.Before(oldestTime) {
			oldestTime = wr.CreatedAt
			oldestSHA = wr.HeadSHA
		}
	}

	if oldestSHA == "" {
		return nil, errors.New("no workflow runs with commit SHAs found")
	}

	// Get commit ordering from git log
	fmt.Printf("Building commit order from %s..HEAD\n", oldestSHA[:7])
	commitOrderSlice, err := commitOrder(ctx, oldestSHA)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit order: %w", err)
	}
	fmt.Printf("Commit range: %d commits\n", len(commitOrderSlice))

	// Select qualifying flaky tests
	targetTests := selectTopFlakyTests(allRuns, cfg)
	if cfg.TopN > 0 {
		fmt.Printf("Selected %d tests for bisect analysis (top %d, min %d failures, min %d runs)\n",
			len(targetTests), cfg.TopN, cfg.MinFailures, cfg.MinRuns)
	} else {
		fmt.Printf("Selected %d tests for bisect analysis (all qualifying, min %d failures, min %d runs)\n",
			len(targetTests), cfg.MinFailures, cfg.MinRuns)
	}

	// Run bisect for each test
	reports := make([]TestBisectReport, 0, len(targetTests))
	for _, testName := range targetTests {
		fmt.Printf("  Bisecting: %s\n", testName)
		reports = append(reports, runBisectForTest(ctx, cfg, testName, allRuns, commitOrderSlice, runToSHA))
	}

	return reports, nil
}
