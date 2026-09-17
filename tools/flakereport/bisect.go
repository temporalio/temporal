package flakereport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/github"
	"golang.org/x/sync/errgroup"
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

type testRunIndex struct {
	tests map[string]*indexedTestRuns
}

type indexedTestRuns struct {
	totalRuns int
	failures  int
}

type commitRunCounts struct {
	passes int
	fails  int
}

// buildTestRunIndex aggregates non-skipped runs by normalized test name.
// totalRuns and failures count every run, including ones outside the commit range.
func buildTestRunIndex(allRuns []TestRun) *testRunIndex {
	index := &testRunIndex{
		tests: make(map[string]*indexedTestRuns),
	}
	for _, run := range allRuns {
		if run.Skipped {
			continue
		}
		testName := normalizeTestName(run.Name)
		testRuns := index.tests[testName]
		if testRuns == nil {
			testRuns = &indexedTestRuns{}
			index.tests[testName] = testRuns
		}
		testRuns.totalRuns++
		if run.Failed {
			testRuns.failures++
		}
	}
	return index
}

// buildObservationsByTest aggregates selected tests by commit. commitOrder lists
// SHAs oldest to newest; runs with commits outside that range are excluded.
func buildObservationsByTest(
	allRuns []TestRun,
	testNames []string,
	commitOrder []string,
	runToSHA map[int64]string,
) map[string][]CommitObservation {
	selectedTests := make(map[string]struct{}, len(testNames))
	for _, testName := range testNames {
		selectedTests[testName] = struct{}{}
	}

	commitIndexes := make(map[string]int, len(commitOrder))
	for i, sha := range commitOrder {
		commitIndexes[sha] = i
	}

	countsByTest := make(map[string]map[int]commitRunCounts, len(testNames))
	for _, run := range allRuns {
		if run.Skipped {
			continue
		}
		testName := normalizeTestName(run.Name)
		if _, ok := selectedTests[testName]; !ok {
			continue
		}
		commitSHA, ok := runToSHA[run.RunID]
		if !ok || commitSHA == "" {
			continue
		}
		commitIndex, ok := commitIndexes[commitSHA]
		if !ok {
			continue
		}
		countsByCommit := countsByTest[testName]
		if countsByCommit == nil {
			countsByCommit = make(map[int]commitRunCounts)
			countsByTest[testName] = countsByCommit
		}
		counts := countsByCommit[commitIndex]
		if run.Failed {
			counts.fails++
		} else {
			counts.passes++
		}
		countsByCommit[commitIndex] = counts
	}

	observationsByTest := make(map[string][]CommitObservation, len(countsByTest))
	for testName, countsByCommit := range countsByTest {
		commitIndexes := make([]int, 0, len(countsByCommit))
		for commitIndex := range countsByCommit {
			commitIndexes = append(commitIndexes, commitIndex)
		}
		sort.Ints(commitIndexes)

		observations := make([]CommitObservation, 0, len(commitIndexes))
		for _, commitIndex := range commitIndexes {
			counts := countsByCommit[commitIndex]
			observations = append(observations, CommitObservation{
				CommitSHA: commitOrder[commitIndex],
				CommitIdx: commitIndex,
				Prior:     1.0,
				Passes:    counts.passes,
				Fails:     counts.fails,
			})
		}
		observationsByTest[testName] = observations
	}
	return observationsByTest
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
func commitPriorWeight(meta github.Commit, testName string) (weight float64, note string) {
	files := meta.Filenames()
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

// runBisectForTest runs the full bisect pipeline for a single test name.
// commitMetas is a pre-populated, read-only cache of commit metadata (SHA → github.Commit).
//
//nolint:revive // Keep the bisect pipeline linear so the filtering steps remain reviewable.
func runBisectForTest(cfg BisectConfig, testName string, obs []CommitObservation, commitMetas map[string]github.Commit) TestBisectReport {

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

	// Apply heuristics from the pre-fetched commit metadata cache.
	for i := range obs {
		if meta, ok := commitMetas[obs[i].CommitSHA]; ok {
			obs[i].Prior, obs[i].HeuristicNote = commitPriorWeight(meta, testName)
		}
	}

	results := runBisect(obs)
	if results == nil {
		return TestBisectReport{
			TestName: testName,
			Skipped:  true,
		}
	}

	// Build a SHA → note lookup from the obs slice (already computed above).
	obsNotes := make(map[string]string, len(obs))
	for _, o := range obs {
		obsNotes[o.CommitSHA] = o.HeuristicNote
	}

	// Annotate results with commit metadata
	for i := range results {
		results[i].HeuristicNote = obsNotes[results[i].CommitSHA]
		if meta, ok := commitMetas[results[i].CommitSHA]; ok {
			results[i].CommitTitle = meta.Title()
			results[i].CommitAuthor = meta.Commit.Author.Name
			if !meta.Commit.Author.Date.IsZero() {
				results[i].CommitDate = meta.Commit.Author.Date.Format("2006-01-02")
			}
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

	// Filter suspects where the failure rate did not increase at the transition commit.
	// A decreasing failure rate indicates the commit fixed a flake, not introduced one,
	// and is not actionable as a culprit.
	{
		filtered := results[:0]
		for _, r := range results {
			nBefore := r.PassesBefore + r.FailsBefore
			nAfter := r.PassesAfter + r.FailsAfter
			rateBefore := 0.0
			if nBefore > 0 {
				rateBefore = float64(r.FailsBefore) / float64(nBefore)
			}
			rateAfter := 0.0
			if nAfter > 0 {
				rateAfter = float64(r.FailsAfter) / float64(nAfter)
			}
			if rateAfter > rateBefore {
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

// selectTopFlakyTests selects the top-N flakiest indexed tests that meet minimum signal thresholds.
func selectTopFlakyTests(index *testRunIndex, cfg BisectConfig) []string {
	type ranked struct {
		name  string
		rate  float64
		fails int
	}
	var candidates []ranked
	for name, testRuns := range index.tests {
		if testRuns.failures < cfg.MinFailures || testRuns.totalRuns < cfg.MinRuns {
			continue
		}
		candidates = append(candidates, ranked{
			name:  name,
			rate:  float64(testRuns.failures) / float64(testRuns.totalRuns),
			fails: testRuns.failures,
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
func runBisectAnalysis(ctx context.Context, cfg BisectConfig, allRuns []TestRun, githubActionsRuns []github.Run) ([]TestBisectReport, error) {
	runToSHA := make(map[int64]string, len(githubActionsRuns))
	var oldestSHA string
	var oldestTime time.Time
	for _, githubActionsRun := range githubActionsRuns {
		if githubActionsRun.HeadSHA == "" {
			continue
		}
		runToSHA[githubActionsRun.DatabaseID] = githubActionsRun.HeadSHA
		if oldestTime.IsZero() || githubActionsRun.CreatedAt.Before(oldestTime) {
			oldestTime = githubActionsRun.CreatedAt
			oldestSHA = githubActionsRun.HeadSHA
		}
	}

	if oldestSHA == "" {
		return nil, errors.New("no GitHub Actions runs with commit SHAs found")
	}

	// Get commit ordering from git log
	fmt.Printf("Building commit order from %s..HEAD\n", oldestSHA[:7])
	commitOrderSlice, err := commitOrder(ctx, oldestSHA)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit order: %w", err)
	}
	if len(commitOrderSlice) == 0 {
		return nil, fmt.Errorf("commit order is empty for anchor %s: SHA may not be an ancestor of HEAD (force-push or unrelated branch?)", oldestSHA[:7])
	}
	fmt.Printf("Commit range: %d commits\n", len(commitOrderSlice))
	testRuns := buildTestRunIndex(allRuns)

	// Select qualifying flaky tests
	targetTests := selectTopFlakyTests(testRuns, cfg)
	if cfg.TopN > 0 {
		fmt.Printf("Selected %d tests for bisect analysis (top %d, min %d failures, min %d runs)\n",
			len(targetTests), cfg.TopN, cfg.MinFailures, cfg.MinRuns)
	} else {
		fmt.Printf("Selected %d tests for bisect analysis (all qualifying, min %d failures, min %d runs)\n",
			len(targetTests), cfg.MinFailures, cfg.MinRuns)
	}

	observationsByTest := buildObservationsByTest(allRuns, targetTests, commitOrderSlice, runToSHA)

	// Fetching once here, deduplicated and in parallel, avoids O(tests × commits)
	// serial subprocess calls.
	uniqueSHAs := make([]string, 0, len(runToSHA))
	seenSHA := make(map[string]struct{}, len(runToSHA))
	for _, sha := range runToSHA {
		if _, ok := seenSHA[sha]; !ok {
			seenSHA[sha] = struct{}{}
			uniqueSHAs = append(uniqueSHAs, sha)
		}
	}
	commitMetas, err := fetchCommitMetasParallel(ctx, cfg.Repo, uniqueSHAs, defaultConcurrency)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Fetched metadata for %d/%d unique commits\n", len(commitMetas), len(uniqueSHAs))

	// Run bisect for each test
	reports := make([]TestBisectReport, 0, len(targetTests))
	for _, testName := range targetTests {
		start := time.Now()
		report := runBisectForTest(cfg, testName, observationsByTest[testName], commitMetas)
		fmt.Printf("  Bisected %s in %s (skipped=%v)\n", testName, time.Since(start).Round(time.Millisecond), report.Skipped)
		reports = append(reports, report)
	}

	return reports, nil
}

// fetchCommitMetasParallel fetches commit metadata for a list of SHAs concurrently,
// bounded by maxConcurrency. Returns an immutable map of SHA → github.Commit; non-rate-limit
// fetch failures are logged and omitted because heuristic priors fall back to 1.0.
func fetchCommitMetasParallel(ctx context.Context, repo string, shas []string, maxConcurrency int) (map[string]github.Commit, error) {
	metas := make([]github.Commit, len(shas))
	ok := make([]bool, len(shas))

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrency)
	for i, sha := range shas {
		g.Go(func() error {
			meta, err := github.GetCommit(ctx, repo, sha)
			if err != nil {
				if isGitHubRateLimitError(err) {
					return fmt.Errorf("GitHub API rate limit while fetching metadata for commit %s: %w", sha[:7], err)
				}
				fmt.Printf("Warning: failed to fetch metadata for commit %s: %v\n", sha[:7], err)
				return nil
			}
			metas[i] = meta
			ok[i] = true
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("cannot generate complete report: %w", err)
	}

	commitMetas := make(map[string]github.Commit, len(shas))
	for i, sha := range shas {
		if ok[i] {
			commitMetas[sha] = metas[i]
		}
	}
	return commitMetas, nil
}
