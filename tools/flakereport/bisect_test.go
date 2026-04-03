package flakereport

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogBetaBinomial(t *testing.T) {
	t.Run("symmetry: k=0 equals k=n", func(t *testing.T) {
		// logBetaBinomial(n, 0) == logBetaBinomial(n, n) by symmetry of Beta(1,1)
		v0 := logBetaBinomial(10, 0, 1, 1)
		v10 := logBetaBinomial(10, 10, 1, 1)
		assert.InDelta(t, v0, v10, 1e-10, "logBetaBinomial(10,0,1,1) should equal logBetaBinomial(10,10,1,1)")
	})

	t.Run("extreme k is more likely than middle k under Beta(1,1)", func(t *testing.T) {
		// Under a Beta(1,1) (uniform) prior, the marginal likelihood favors extreme k.
		// Observing all failures (k=n=10) or all passes (k=0) is MORE likely than k=5,
		// because the posterior can concentrate near p=1 or p=0 to explain the data.
		vMid := logBetaBinomial(10, 5, 1, 1)
		v0 := logBetaBinomial(10, 0, 1, 1)
		assert.Greater(t, v0, vMid, "logBetaBinomial(10,0,1,1) should exceed logBetaBinomial(10,5,1,1) under uniform prior")
	})

	t.Run("n=0 k=0 is valid", func(t *testing.T) {
		// No observations: should return a finite value (0 with Beta-Binomial normalization)
		v := logBetaBinomial(0, 0, 1, 1)
		assert.False(t, math.IsNaN(v), "logBetaBinomial(0,0,1,1) should not be NaN")
		assert.False(t, math.IsInf(v, 0), "logBetaBinomial(0,0,1,1) should not be Inf")
	})

	t.Run("single failure in 1 trial", func(t *testing.T) {
		v := logBetaBinomial(1, 1, 1, 1)
		assert.False(t, math.IsNaN(v), "logBetaBinomial(1,1,1,1) should not be NaN")
		assert.False(t, math.IsInf(v, 0), "logBetaBinomial(1,1,1,1) should not be Inf")
	})
}

func TestLogSumExpNormalize(t *testing.T) {
	t.Run("empty input returns nil", func(t *testing.T) {
		result := logSumExpNormalize(nil)
		assert.Nil(t, result)
	})

	t.Run("single element returns 1.0", func(t *testing.T) {
		result := logSumExpNormalize([]float64{-5.0})
		require.Len(t, result, 1)
		assert.InDelta(t, 1.0, result[0], 1e-10)
	})

	t.Run("uniform log-weights produce equal probabilities", func(t *testing.T) {
		logWeights := []float64{-3.0, -3.0, -3.0, -3.0}
		result := logSumExpNormalize(logWeights)
		require.Len(t, result, 4)
		for _, p := range result {
			assert.InDelta(t, 0.25, p, 1e-10)
		}
	})

	t.Run("probabilities sum to 1.0", func(t *testing.T) {
		logWeights := []float64{-10.0, -5.0, -1.0, -3.0, -7.0}
		result := logSumExpNormalize(logWeights)
		require.Len(t, result, 5)
		sum := 0.0
		for _, p := range result {
			sum += p
		}
		assert.InDelta(t, 1.0, sum, 1e-10)
	})

	t.Run("highest log-weight gets highest probability", func(t *testing.T) {
		logWeights := []float64{-10.0, -5.0, -1.0, -3.0}
		result := logSumExpNormalize(logWeights)
		require.Len(t, result, 4)
		// index 2 has the highest log-weight (-1.0)
		maxIdx := 0
		for i, p := range result {
			if p > result[maxIdx] {
				maxIdx = i
			}
		}
		assert.Equal(t, 2, maxIdx)
	})

	t.Run("handles very large negative values without underflow", func(t *testing.T) {
		logWeights := []float64{-1000.0, -999.0, -998.0}
		result := logSumExpNormalize(logWeights)
		require.Len(t, result, 3)
		sum := 0.0
		for _, p := range result {
			assert.False(t, math.IsNaN(p), "probability should not be NaN")
			assert.False(t, math.IsInf(p, 0), "probability should not be Inf")
			assert.GreaterOrEqual(t, p, 0.0, "probability should be non-negative")
			sum += p
		}
		assert.InDelta(t, 1.0, sum, 1e-10)
	})
}

func TestRunBisect(t *testing.T) {
	t.Run("returns nil with fewer than 2 observations", func(t *testing.T) {
		obs := []CommitObservation{
			{CommitSHA: "abc", CommitIdx: 0, Prior: 1.0, Passes: 10, Fails: 2},
		}
		result := runBisect(obs)
		assert.Nil(t, result)
	})

	t.Run("returns nil with zero total observations", func(t *testing.T) {
		obs := []CommitObservation{
			{CommitSHA: "abc", CommitIdx: 0, Prior: 1.0, Passes: 0, Fails: 0},
			{CommitSHA: "def", CommitIdx: 1, Prior: 1.0, Passes: 0, Fails: 0},
		}
		result := runBisect(obs)
		assert.Nil(t, result)
	})

	t.Run("sharp signal: transition at commit index 3", func(t *testing.T) {
		// First 3 commits: 0 fails out of 30 each
		// Last 3 commits: 10 fails out of 30 each (1/3 failure rate)
		// The 4th commit (index 3) should have the highest posterior probability
		obs := []CommitObservation{
			{CommitSHA: "sha0", CommitIdx: 0, Prior: 1.0, Passes: 30, Fails: 0},
			{CommitSHA: "sha1", CommitIdx: 1, Prior: 1.0, Passes: 30, Fails: 0},
			{CommitSHA: "sha2", CommitIdx: 2, Prior: 1.0, Passes: 30, Fails: 0},
			{CommitSHA: "sha3", CommitIdx: 3, Prior: 1.0, Passes: 20, Fails: 10},
			{CommitSHA: "sha4", CommitIdx: 4, Prior: 1.0, Passes: 20, Fails: 10},
			{CommitSHA: "sha5", CommitIdx: 5, Prior: 1.0, Passes: 20, Fails: 10},
		}
		results := runBisect(obs)
		require.NotNil(t, results)
		require.Len(t, results, 6)

		// Probabilities should sum to 1.0
		sum := 0.0
		for _, r := range results {
			sum += r.Probability
		}
		assert.InDelta(t, 1.0, sum, 1e-10)

		// The top result should be sha3 (index 3) — the transition point
		require.Equal(t, "sha3", results[0].CommitSHA, "sha3 should be the top suspect")
		require.Greater(t, results[0].Probability, 0.5, "top suspect should have >50% probability")

		// Results should be sorted by probability descending
		for i := 1; i < len(results); i++ {
			assert.LessOrEqual(t, results[i].Probability, results[i-1].Probability,
				"results should be sorted by probability descending")
		}
	})

	t.Run("counts are correctly propagated", func(t *testing.T) {
		obs := []CommitObservation{
			{CommitSHA: "sha0", CommitIdx: 0, Prior: 1.0, Passes: 10, Fails: 0},
			{CommitSHA: "sha1", CommitIdx: 1, Prior: 1.0, Passes: 5, Fails: 5},
		}
		results := runBisect(obs)
		require.NotNil(t, results)

		// Find sha1 result (should be the top suspect since all failures are after it)
		var sha1Result *BisectResult
		for i := range results {
			if results[i].CommitSHA == "sha1" {
				sha1Result = &results[i]
				break
			}
		}
		require.NotNil(t, sha1Result)
		// sha1 is the transition: before sha1 = 10 passes, 0 fails; after/at sha1 = 5 passes, 5 fails
		assert.Equal(t, 10, sha1Result.PassesBefore)
		assert.Equal(t, 0, sha1Result.FailsBefore)
		assert.Equal(t, 5, sha1Result.PassesAfter)
		assert.Equal(t, 5, sha1Result.FailsAfter)
	})

	t.Run("two commits uniform signal returns valid probabilities", func(t *testing.T) {
		obs := []CommitObservation{
			{CommitSHA: "sha0", CommitIdx: 0, Prior: 1.0, Passes: 5, Fails: 5},
			{CommitSHA: "sha1", CommitIdx: 1, Prior: 1.0, Passes: 5, Fails: 5},
		}
		results := runBisect(obs)
		require.NotNil(t, results)
		require.Len(t, results, 2)
		sum := results[0].Probability + results[1].Probability
		assert.InDelta(t, 1.0, sum, 1e-10)
	})
}

func TestRunBisectInformationlessData(t *testing.T) {
	t.Run("zero observations across many commits returns nil", func(t *testing.T) {
		// With no observations at any commit, there is no data to localize a transition.
		obs := []CommitObservation{
			{CommitSHA: "sha0", CommitIdx: 0, Prior: 1.0},
			{CommitSHA: "sha1", CommitIdx: 1, Prior: 1.0},
			{CommitSHA: "sha2", CommitIdx: 2, Prior: 1.0},
			{CommitSHA: "sha3", CommitIdx: 3, Prior: 1.0},
			{CommitSHA: "sha4", CommitIdx: 4, Prior: 1.0},
		}
		result := runBisect(obs)
		assert.Nil(t, result)
	})

	t.Run("balanced data (equal failures and successes per commit) yields symmetric posterior", func(t *testing.T) {
		// Each commit has the same number of failures and successes (50% failure rate
		// throughout), so there is no directional signal about where a transition occurred.
		//
		// The Beta-Binomial model does NOT produce posterior == prior in this case: placing
		// all observations on one side of the transition is more parsimonious (one marginal
		// distribution instead of two), so the model assigns slightly higher likelihood to
		// extreme transition points. However, the posterior IS symmetric around the midpoint:
		// prob[i] == prob[N-1-i] when priors are uniform and per-commit observations are
		// identical.
		obs := []CommitObservation{
			{CommitSHA: "sha0", CommitIdx: 0, Prior: 1.0, Passes: 5, Fails: 5},
			{CommitSHA: "sha1", CommitIdx: 1, Prior: 1.0, Passes: 5, Fails: 5},
			{CommitSHA: "sha2", CommitIdx: 2, Prior: 1.0, Passes: 5, Fails: 5},
			{CommitSHA: "sha3", CommitIdx: 3, Prior: 1.0, Passes: 5, Fails: 5},
			{CommitSHA: "sha4", CommitIdx: 4, Prior: 1.0, Passes: 5, Fails: 5},
		}
		results := runBisect(obs)
		require.NotNil(t, results)
		require.Len(t, results, 5)

		// Probabilities must be non-negative and sum to 1.0.
		sum := 0.0
		for _, r := range results {
			assert.GreaterOrEqual(t, r.Probability, 0.0)
			sum += r.Probability
		}
		assert.InDelta(t, 1.0, sum, 1e-10)

		// With uniform priors and identical per-commit observations the distribution has
		// a specific symmetry: the weight for transition at index i depends only on the
		// total observations before/after the split. Because the "before" segment for
		// index i mirrors the "after" segment for index N-1-i+1, the pairs (sha1, sha4)
		// and (sha2, sha3) must have equal probability. sha0 is unpaired: its "before"
		// segment is empty while no index has an empty "after" segment.
		//
		// Note: sha0 will have a higher probability than the others because placing all
		// observations on one side of the transition is more parsimonious for the
		// Beta-Binomial model than splitting them between two equal-rate distributions.
		// The balanced data does not produce a diffuse uniform posterior.
		probBySHA := make(map[string]float64, len(results))
		for _, r := range results {
			probBySHA[r.CommitSHA] = r.Probability
		}
		// No commit should dominate with balanced data (>50% would imply a spurious signal).
		for sha, p := range probBySHA {
			assert.Less(t, p, 0.5, "no single commit should dominate with balanced data (sha=%s)", sha)
		}
		// Paired commits must have equal probability.
		assert.InDelta(t, probBySHA["sha1"], probBySHA["sha4"], 1e-10,
			"symmetric pair: sha1 and sha4 should have equal probability")
		assert.InDelta(t, probBySHA["sha2"], probBySHA["sha3"], 1e-10,
			"symmetric pair: sha2 and sha3 should have equal probability")
	})
}

func TestBuildObservations(t *testing.T) {
	commitOrderSlice := []string{"sha-a", "sha-b", "sha-c", "sha-d"}
	runToSHA := map[int64]string{
		100: "sha-a",
		200: "sha-b",
		300: "sha-c",
		400: "sha-d",
		500: "sha-unknown", // not in commitOrder
	}

	runs := []TestRun{
		// sha-a: 3 passes, 1 fail for "TestFoo"
		{Name: "TestFoo", Failed: false, RunID: 100},
		{Name: "TestFoo", Failed: false, RunID: 100},
		{Name: "TestFoo", Failed: false, RunID: 100},
		{Name: "TestFoo", Failed: true, RunID: 100},
		// sha-b: 2 passes for "TestFoo"
		{Name: "TestFoo", Failed: false, RunID: 200},
		{Name: "TestFoo", Failed: false, RunID: 200},
		// sha-c: 1 pass, 2 fails for "TestFoo"
		{Name: "TestFoo", Failed: false, RunID: 300},
		{Name: "TestFoo", Failed: true, RunID: 300},
		{Name: "TestFoo", Failed: true, RunID: 300},
		// sha-d: 1 fail for "TestFoo" with retry suffix (should normalize to "TestFoo")
		{Name: "TestFoo (retry 1)", Failed: true, RunID: 400},
		// sha-unknown: should be skipped (not in commitOrder)
		{Name: "TestFoo", Failed: false, RunID: 500},
		// different test name: should be excluded
		{Name: "TestBar", Failed: true, RunID: 100},
		// skipped run: should be excluded
		{Name: "TestFoo", Failed: false, Skipped: true, RunID: 100},
		// no SHA mapping: should be excluded
		{Name: "TestFoo", Failed: false, RunID: 999},
	}

	obs := buildObservations("TestFoo", runs, commitOrderSlice, runToSHA)

	require.Len(t, obs, 4, "should have one observation per commit with data")

	// Verify chronological ordering
	for i := 1; i < len(obs); i++ {
		assert.Less(t, obs[i-1].CommitIdx, obs[i].CommitIdx,
			"observations should be sorted by commit index")
	}

	// Find each commit's observation
	byShA := make(map[string]CommitObservation)
	for _, o := range obs {
		byShA[o.CommitSHA] = o
	}

	// sha-a: 3 passes, 1 fail
	require.Contains(t, byShA, "sha-a")
	assert.Equal(t, 3, byShA["sha-a"].Passes)
	assert.Equal(t, 1, byShA["sha-a"].Fails)
	assert.Equal(t, 0, byShA["sha-a"].CommitIdx)

	// sha-b: 2 passes, 0 fails
	require.Contains(t, byShA, "sha-b")
	assert.Equal(t, 2, byShA["sha-b"].Passes)
	assert.Equal(t, 0, byShA["sha-b"].Fails)
	assert.Equal(t, 1, byShA["sha-b"].CommitIdx)

	// sha-c: 1 pass, 2 fails
	require.Contains(t, byShA, "sha-c")
	assert.Equal(t, 1, byShA["sha-c"].Passes)
	assert.Equal(t, 2, byShA["sha-c"].Fails)
	assert.Equal(t, 2, byShA["sha-c"].CommitIdx)

	// sha-d: 0 passes, 1 fail (the retry suffix is normalized to "TestFoo")
	require.Contains(t, byShA, "sha-d")
	assert.Equal(t, 0, byShA["sha-d"].Passes)
	assert.Equal(t, 1, byShA["sha-d"].Fails)
	assert.Equal(t, 3, byShA["sha-d"].CommitIdx)

	// sha-unknown should not appear
	assert.NotContains(t, byShA, "sha-unknown")

	// Prior should default to 1.0
	for _, o := range obs {
		assert.InDelta(t, 1.0, o.Prior, 1e-10)
	}
}

func TestSelectTopFlakyTests(t *testing.T) {
	t.Run("excludes tests below MinFailures threshold", func(t *testing.T) {
		runs := makeRunsForTest("TestLowFails", 100, 2) // 2 failures: below minBisectFailures=5
		cfg := BisectConfig{TopN: 10, MinFailures: 5, MinRuns: 10}
		result := selectTopFlakyTests(runs, cfg)
		assert.NotContains(t, result, "TestLowFails")
	})

	t.Run("excludes tests below MinRuns threshold", func(t *testing.T) {
		runs := makeRunsForTest("TestFewRuns", 10, 6) // only 10 total runs: below minBisectRuns=30
		cfg := BisectConfig{TopN: 10, MinFailures: 5, MinRuns: 30}
		result := selectTopFlakyTests(runs, cfg)
		assert.NotContains(t, result, "TestFewRuns")
	})

	t.Run("ranks by failure rate descending", func(t *testing.T) {
		// TestHighRate: 20/40 = 50% failure rate
		// TestLowRate: 10/40 = 25% failure rate
		var runs []TestRun
		runs = append(runs, makeRunsForTest("TestHighRate", 40, 20)...)
		runs = append(runs, makeRunsForTest("TestLowRate", 40, 10)...)
		cfg := BisectConfig{TopN: 10, MinFailures: 5, MinRuns: 30}
		result := selectTopFlakyTests(runs, cfg)
		require.GreaterOrEqual(t, len(result), 2)
		assert.Equal(t, "TestHighRate", result[0])
		assert.Equal(t, "TestLowRate", result[1])
	})

	t.Run("limits to TopN results", func(t *testing.T) {
		var runs []TestRun
		for i := range 5 {
			runs = append(runs, makeRunsForTest("TestFlaky"+string(rune('A'+i)), 40, 10)...)
		}
		cfg := BisectConfig{TopN: 3, MinFailures: 5, MinRuns: 30}
		result := selectTopFlakyTests(runs, cfg)
		assert.Len(t, result, 3)
	})

	t.Run("skips skipped runs", func(t *testing.T) {
		runs := []TestRun{
			{Name: "TestSkipped", Skipped: true, Failed: false},
			{Name: "TestSkipped", Skipped: true, Failed: true},
		}
		cfg := BisectConfig{TopN: 10, MinFailures: 1, MinRuns: 1}
		result := selectTopFlakyTests(runs, cfg)
		assert.NotContains(t, result, "TestSkipped")
	})

	t.Run("normalizes test names", func(t *testing.T) {
		// Retry suffix should be stripped; all runs collapse to one test
		var runs []TestRun
		for range 30 {
			runs = append(runs, TestRun{Name: "TestFlaky", Failed: false})
		}
		for range 8 {
			runs = append(runs, TestRun{Name: "TestFlaky (retry 1)", Failed: true})
		}
		cfg := BisectConfig{TopN: 10, MinFailures: 5, MinRuns: 30}
		result := selectTopFlakyTests(runs, cfg)
		require.Len(t, result, 1)
		assert.Equal(t, "TestFlaky", result[0])
	})
}

func TestCommitPriorWeight(t *testing.T) {
	t.Run("docs-only commit is deprioritized", func(t *testing.T) {
		meta := CommitMeta{
			SHA:   "abc",
			Files: []string{".github/workflows/ci.yml", "docs/architecture.md"},
		}
		weight, note := commitPriorWeight(meta, "TestWorkflowSuite/TestRun")
		assert.InDelta(t, 0.05, weight, 1e-10)
		assert.NotEmpty(t, note)
	})

	t.Run("markdown-only commit is deprioritized", func(t *testing.T) {
		meta := CommitMeta{
			SHA:   "abc",
			Files: []string{"README.md", "CHANGELOG.md"},
		}
		weight, note := commitPriorWeight(meta, "TestWorkflowSuite/TestRun")
		assert.InDelta(t, 0.05, weight, 1e-10)
		assert.NotEmpty(t, note)
	})

	t.Run("service/history/ commit is neutral weight", func(t *testing.T) {
		meta := CommitMeta{
			SHA:   "abc",
			Files: []string{"service/history/workflow_executor.go", "service/history/mutable_state.go"},
		}
		weight, note := commitPriorWeight(meta, "TestWorkflowSuite/TestContinueAsNew")
		assert.InDelta(t, 1.0, weight, 1e-10)
		assert.Empty(t, note)
	})

	t.Run("test-only commit is mildly deprioritized", func(t *testing.T) {
		meta := CommitMeta{
			SHA:   "abc",
			Files: []string{"service/frontend/nexus_handler_test.go", "service/worker/deployment_test.go"},
		}
		weight, note := commitPriorWeight(meta, "TestWorkflowSuite/TestRun")
		assert.InDelta(t, 0.3, weight, 1e-10)
		assert.NotEmpty(t, note)
	})

	t.Run("mixed code and doc files is neutral", func(t *testing.T) {
		meta := CommitMeta{
			SHA:   "abc",
			Files: []string{"service/frontend/handler.go", "README.md"},
		}
		weight, note := commitPriorWeight(meta, "TestWorkflowSuite/TestRun")
		// Not all files are docs/config (handler.go is not), not all are test files
		// and "service/history/" is not a prefix of service/frontend/
		assert.InDelta(t, 1.0, weight, 1e-10)
		assert.Empty(t, note)
	})

	t.Run("empty files list returns neutral weight", func(t *testing.T) {
		meta := CommitMeta{SHA: "abc", Files: nil}
		weight, note := commitPriorWeight(meta, "TestWorkflowSuite/TestRun")
		assert.InDelta(t, 1.0, weight, 1e-10)
		assert.Empty(t, note)
	})

	t.Run("service/frontend/ commit is neutral weight", func(t *testing.T) {
		meta := CommitMeta{
			SHA:   "abc",
			Files: []string{"service/frontend/nexus_handler.go"},
		}
		weight, note := commitPriorWeight(meta, "TestNexusSuite/TestOperation")
		assert.InDelta(t, 1.0, weight, 1e-10)
		assert.Empty(t, note)
	})
}

// makeRunsForTest creates a slice of TestRun with the given total count and failure count.
// The first `failures` runs are marked as failed, the rest as passed.
func makeRunsForTest(name string, total, failures int) []TestRun {
	runs := make([]TestRun, total)
	for i := range runs {
		runs[i] = TestRun{
			Name:   name,
			Failed: i < failures,
		}
	}
	return runs
}
