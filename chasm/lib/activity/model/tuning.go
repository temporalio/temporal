package model

// The tuning knobs shared by the two model-conformance explorers: the in-memory engine one
// (chasm/lib/activity/activity_conformance_test.go) and the onebox one
// (tests/activity_standalone_conformance.go). They live here so a knob has one name, one default,
// and one meaning whichever explorer reads it.

import (
	"os"
	"strconv"
)

// MaxDepth is the BFS depth cap, raised by TEMPORAL_SAASPEC_MAX_DEPTH.
func MaxDepth() int { return envInt("TEMPORAL_SAASPEC_MAX_DEPTH", 4) }

// WalkSteps is the random walk's step budget per config, raised by TEMPORAL_SAASPEC_WALK_STEPS.
// Raise TEMPORAL_TEST_TIMEOUT alongside it on the onebox explorer, which pays a real RPC per step.
func WalkSteps() int { return envInt("TEMPORAL_SAASPEC_WALK_STEPS", 200) }

// WalkSeed makes a walk reproducible; it is logged, so a failure replays with
// TEMPORAL_SAASPEC_WALK_SEED.
func WalkSeed() int64 {
	if n, err := strconv.ParseInt(os.Getenv("TEMPORAL_SAASPEC_WALK_SEED"), 10, 64); err == nil {
		return n
	}
	return 1
}

// SkipNegativePoll drops the "a Paused activity must not dispatch" long poll, the dominant cost of
// deep walks on the onebox explorer. The per-edge state check still runs.
func SkipNegativePoll() bool { return os.Getenv("TEMPORAL_SAASPEC_NO_NEGATIVE_POLL") != "" }

// ReportCompleteness additionally prints the reachable cells a run did not exercise.
func ReportCompleteness() bool { return os.Getenv("TEMPORAL_SAASPEC_COMPLETENESS") != "" }

// Verbose logs every step of a walk.
func Verbose() bool { return os.Getenv("TEMPORAL_SAASPEC_VERBOSE") != "" }

func envInt(name string, fallback int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return fallback
}
