package regress_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/regress"
)

func TestRunWritesIncrementalAndCompletedArtifacts(t *testing.T) {
	harness := &artifactHarness{}
	suite := artifactSuite()

	err := regress.Run(context.Background(), suite, harness)
	require.NoError(t, err)
	require.NotEmpty(t, harness.artifacts)

	var beganWithPolicy bool
	for _, artifact := range harness.artifacts {
		path := artifact.Paths[0]
		if len(path.ActionsBegun) == 1 && len(path.ActivePolicies) == 1 {
			beganWithPolicy = true
		}
	}
	require.True(t, beganWithPolicy)

	completed := harness.artifacts[len(harness.artifacts)-1]
	require.True(t, completed.Complete)
	require.True(t, completed.Paths[0].Complete)
	require.Equal(t, regress.Bindings{"job": "job-1"}, completed.Paths[0].Bindings)
	require.Equal(t, []string{"task.state"}, completed.Paths[0].Observations)
	require.Empty(t, completed.Paths[0].ActivePolicies)
	encoded, err := json.Marshal(completed.Paths[0])
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	require.JSONEq(t, `[{"name":"task.observed","payload":{"state":"done"}}]`, string(fields["facts"]))
}

func TestRunCrashArtifactRetainsBegunAction(t *testing.T) {
	harness := &artifactHarness{recordingHarness: recordingHarness{fireErr: errors.New("crash")}}

	err := regress.Run(context.Background(), artifactSuite(), harness)
	require.Error(t, err)
	require.NotEmpty(t, harness.artifacts)

	found := false
	for _, artifact := range harness.artifacts {
		if len(artifact.Paths[0].ActionsBegun) == 1 {
			found = true
			break
		}
	}
	require.True(t, found)
	require.Contains(t, harness.artifacts[len(harness.artifacts)-1].Paths[0].Error, "crash")
}

func TestReplayRejectsModelOrProfileDrift(t *testing.T) {
	artifact := regress.Artifact{
		ModelVersion: "fake/v1",
		Profile:      regress.Profile{Name: "local", Capabilities: []string{"faults"}},
		Completed:    artifactSuite(),
	}
	domain := regress.NewDomain("fake/v2")

	err := regress.Replay(
		context.Background(),
		artifact,
		domain,
		regress.Profile{Name: "local", Capabilities: []string{"faults"}},
		&recordingHarness{},
	)
	require.ErrorIs(t, err, regress.ErrReplayMismatch)
}

func TestReplayRejectsObservedStateDrift(t *testing.T) {
	suite := artifactSuite()
	suite.Profile.ObservedBindings = regress.Bindings{"job": "job-1"}
	artifact := regress.Artifact{
		ModelVersion: suite.ModelVersion,
		Profile:      suite.Profile,
		Completed:    suite,
	}

	err := regress.Replay(
		context.Background(),
		artifact,
		regress.NewDomain(suite.ModelVersion),
		regress.Profile{Name: suite.Profile.Name, Capabilities: suite.Profile.Capabilities, ObservedBindings: regress.Bindings{"job": "job-2"}},
		&recordingHarness{},
	)
	require.ErrorIs(t, err, regress.ErrReplayMismatch)
}

func TestReplayExecutesMatchingCompletedArtifact(t *testing.T) {
	suite := artifactSuite()
	artifact := regress.Artifact{
		ModelVersion: suite.ModelVersion,
		Profile:      suite.Profile,
		Completed:    suite,
	}
	harness := &recordingHarness{}

	require.NoError(t, regress.Replay(context.Background(), artifact, regress.NewDomain("fake/v1"), suite.Profile, harness))
	require.Contains(t, harness.events, "fire:task.finish")
}

func TestJSONFileSinkAtomicallyPersistsArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "artifact.json")
	sink, err := regress.NewJSONFileSink(path)
	require.NoError(t, err)
	want := regress.Artifact{Name: "named-plan", ModelVersion: "fake/v1", Complete: true}

	require.NoError(t, sink.WriteArtifact(context.Background(), want))
	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	var got regress.Artifact
	require.NoError(t, json.Unmarshal(encoded, &got))
	require.Equal(t, want, got)
}

type artifactHarness struct {
	recordingHarness
	artifacts []regress.Artifact
}

func (*recordingPath) ArtifactFacts(context.Context) ([]json.RawMessage, error) {
	return []json.RawMessage{json.RawMessage(`{"name":"task.observed","payload":{"state":"done"}}`)}, nil
}

func (h *artifactHarness) ArtifactSink() regress.ArtifactSink { return h }

func (h *artifactHarness) WriteArtifact(_ context.Context, artifact regress.Artifact) error {
	h.artifacts = append(h.artifacts, artifact)
	return nil
}

func artifactSuite() regress.Suite {
	return regress.Suite{
		ModelVersion: "fake/v1",
		Profile:      regress.Profile{Name: "local", Capabilities: []string{"faults"}},
		IR: regress.IR{
			Mode:    regress.OnePathMode,
			Symbols: regress.Symbols{"job": {Name: "job", Type: taskType}},
		},
		Paths: []regress.CompletedPath{{
			Actions: []regress.CompletedAction{{Name: "task.finish"}},
			Steps: []regress.CompletedStep{{
				Action: regress.CompletedAction{Name: "task.finish"},
				Mode:   regress.ProactiveAction,
			}},
			Policies: []regress.CompletedPolicy{{Name: "rpc.fail-next", Start: 0, End: 1}},
			Milestones: []regress.CompletedMilestone{{
				Kind:        regress.OutcomeKind,
				Name:        "task.state",
				AfterAction: 1,
			}},
		}},
		PathCount: 1,
	}
}
