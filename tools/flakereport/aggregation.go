package flakereport

type testRunSummary struct {
	totalRuns int
	tests     map[string]*summarizedTestRuns
	suiteRuns map[string]map[suiteRunIdentity]struct{}
}

type summarizedTestRuns struct {
	totalRuns     int
	failures      int
	byWorkflowRun map[int64]commitRunCounts
}

type suiteRunIdentity struct {
	runID      int64
	matrixName string
}

func newTestRunSummary() *testRunSummary {
	return &testRunSummary{
		tests:     make(map[string]*summarizedTestRuns),
		suiteRuns: make(map[string]map[suiteRunIdentity]struct{}),
	}
}

func (s *testRunSummary) add(runs []TestRun) {
	s.totalRuns += len(runs)
	for _, run := range runs {
		if run.Skipped {
			continue
		}

		testName := normalizeTestName(run.Name)
		testRuns := s.tests[testName]
		if testRuns == nil {
			testRuns = &summarizedTestRuns{byWorkflowRun: make(map[int64]commitRunCounts)}
			s.tests[testName] = testRuns
		}
		testRuns.totalRuns++
		counts := testRuns.byWorkflowRun[run.RunID]
		if run.Failed {
			testRuns.failures++
			counts.fails++
		} else {
			counts.passes++
		}
		testRuns.byWorkflowRun[run.RunID] = counts

		if isGoTestSuite(run.SuiteName) {
			if s.suiteRuns[run.SuiteName] == nil {
				s.suiteRuns[run.SuiteName] = make(map[suiteRunIdentity]struct{})
			}
			s.suiteRuns[run.SuiteName][suiteRunIdentity{runID: run.RunID, matrixName: run.MatrixName}] = struct{}{}
		}
	}
}

func (s *testRunSummary) merge(other *testRunSummary) {
	if other == nil {
		return
	}
	s.totalRuns += other.totalRuns
	for testName, otherRuns := range other.tests {
		testRuns := s.tests[testName]
		if testRuns == nil {
			testRuns = &summarizedTestRuns{byWorkflowRun: make(map[int64]commitRunCounts)}
			s.tests[testName] = testRuns
		}
		testRuns.totalRuns += otherRuns.totalRuns
		testRuns.failures += otherRuns.failures
		for runID, otherCounts := range otherRuns.byWorkflowRun {
			counts := testRuns.byWorkflowRun[runID]
			counts.passes += otherCounts.passes
			counts.fails += otherCounts.fails
			testRuns.byWorkflowRun[runID] = counts
		}
	}
	for suiteName, otherRuns := range other.suiteRuns {
		if s.suiteRuns[suiteName] == nil {
			s.suiteRuns[suiteName] = make(map[suiteRunIdentity]struct{})
		}
		for run := range otherRuns {
			s.suiteRuns[suiteName][run] = struct{}{}
		}
	}
}

func (s *testRunSummary) countsByTest() map[string]int {
	counts := make(map[string]int, len(s.tests))
	for testName, runs := range s.tests {
		counts[testName] = runs.totalRuns
	}
	return counts
}
