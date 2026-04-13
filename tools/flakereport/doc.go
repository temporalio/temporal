// Package flakereport implements Bayesian commit bisection for identifying which commit
// most likely introduced a flaky test in a CI system.
//
// # Problem
//
// A flaky test is one whose failure probability changes at some point in the commit
// history — typically because a code change introduced a race condition, timing
// sensitivity, or environmental dependency. Given a rolling window of CI runs (each
// associated with the HEAD commit at the time), we want to rank candidate commits by
// their posterior probability of being the "transition commit" that caused the flakiness.
//
// # Data
//
// The raw input is a set of test runs downloaded from GitHub Actions artifacts. Each run
// records whether a specific test passed or failed, and is tagged with the workflow RunID.
// RunIDs are mapped to commit SHAs via the GitHub Actions API (WorkflowRun.HeadSHA).
// Runs are then grouped into CommitObservation records: for each (test, commit SHA) pair
// we count the number of passing and failing runs.
//
// # Model
//
// The inference model assumes exactly one transition commit c in the history:
//
//   - Before commit c: the test has a background failure probability p_before.
//   - At commit c and all later commits: the test has an elevated failure probability p_after.
//
// Neither p_before nor p_after is known, so both are assigned independent uniform (Beta(1,1))
// priors. The model treats them as nuisance parameters and marginalizes them out, yielding
// the Beta-Binomial marginal likelihood:
//
//	P(data | transition at c) = BetaBinomial(n_before, k_before; 1, 1)
//	                           × BetaBinomial(n_after,  k_after;  1, 1)
//
// where n_before / k_before are the total runs / failures before commit c, and n_after /
// k_after are the total runs / failures at and after commit c. The closed form is:
//
//	BetaBinomial(n, k; α, β) = Beta(k+α, n-k+β) / Beta(α, β)
//
// All arithmetic is performed in log-space to avoid floating-point underflow, with the
// log-sum-exp trick applied during normalization.
//
// # Commit Priors
//
// The prior probability that a given commit is the transition commit is not uniform across
// all commits. Commits that only touch documentation, CI configuration, or test files
// cannot plausibly affect production code behaviour, so they receive a reduced prior weight
// (down to 0.05×). Commits that touch source code receive the default weight of 1.0.
// These heuristic weights are fetched from the GitHub API in parallel and applied before
// running the inference.
//
// # Inference Algorithm
//
// For N commits with observations, the algorithm runs in O(N) time using prefix sums:
//
//  1. Compute prefix sums of failures and passes across the commit sequence.
//  2. For each candidate transition commit i, use the prefix sums to split the data into
//     "before" and "after" segments and evaluate the log marginal likelihood.
//  3. Add the log prior weight to each log-likelihood.
//  4. Normalize via log-sum-exp to obtain posterior probabilities that sum to 1.
//
// # Output
//
// Commits are ranked by posterior probability. Only commits above a configurable minimum
// probability threshold (default 0.50) are reported. Results are surfaced in the GitHub
// Actions step summary, a markdown report artifact, and optionally a Slack message
// listing the "hottest" commits (those with the highest aggregate posterior probability
// across all analyzed tests).
//
// # Limitations
//
// The model assumes a single transition in the observed window. If multiple commits each
// contributed independently to flakiness, or if the failure rate oscillates, the model
// may identify the wrong commit or produce a diffuse posterior with no strong suspect.
// The model also requires a minimum of 5 failures and 30 total runs before it will
// attempt bisection on a test, to avoid over-fitting noisy data.
package flakereport
