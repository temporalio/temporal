package runner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/verify"
)

func TestTLAStateDecodersMapRelationTuples(t *testing.T) {
	request := runnerRelationDecoderRequest()
	want := verify.ModelState{
		Entities: map[string]map[string]string{
			"source": {"source#0": ""},
			"target": {"target#0": ""},
		},
		Relations: map[string][]verify.RelationTuple{
			"link": {{Source: "source#0", Target: "target#0"}},
		},
	}

	tlc, err := decodeTLCTrace(request, `State 1: <Initial predicate>
/\ exists_source = {"source#0"}
/\ exists_target = {"target#0"}
/\ relation_link = {<<"source#0", "target#0">>}`)
	require.NoError(t, err)
	require.Equal(t, want, *tlc.Initial)

	itf, err := decodeITFTrace(request, `{
  "#meta": {"format": "ITF"},
  "vars": ["exists_source", "exists_target", "relation_link"],
  "states": [{
	"sourceIDs": {"#set": ["source#0"]},
	"targetIDs": {"#set": ["target#0"]},
    "exists_source": {"#set": ["source#0"]},
    "exists_target": {"#set": ["target#0"]},
    "relation_link": {"#set": [{"#tup": ["source#0", "target#0"]}]}
  }]
}`)
	require.NoError(t, err)
	require.Equal(t, want, *itf.Initial)
}

func TestTLAStateDecodersRejectPartiallyParsedCollections(t *testing.T) {
	_, err := decodeTLAStringSet(`{"source#0", invalid}`)
	require.Error(t, err)

	_, err = decodeTLAFunction(`("source#0" :> "ready" @@ invalid)`, verify.TraceVocabulary{})
	require.Error(t, err)

	_, err = decodeTLARelation(`{<<"source#0", "target#0">>, invalid}`, nil)
	require.Error(t, err)
}

func TestTLCActionDecoderMapsNativeBindings(t *testing.T) {
	step, bindings, err := decodeTLCAction(Request{
		Model: verify.Model{Actions: []verify.Action{{
			Name: "schedule", Parameters: []verify.Parameter{{Name: "operation"}},
		}}},
		TraceVocabulary: verify.TraceVocabulary{
			Actions:    map[string]string{"Action_schedule": "schedule"},
			Bindings:   map[string]map[string]string{"Action_schedule": {"op": "operation"}},
			Identities: map[string]string{"operation_0": "operation#0"},
		},
	}, `Action_schedule(op = "operation_0")`)
	require.NoError(t, err)
	require.Equal(t, "schedule", step)
	require.Equal(t, verify.Bindings{"operation": "operation#0"}, bindings)
}

func TestTLCActionDecoderLeavesOmittedBindingsForStateInference(t *testing.T) {
	action, bindings, err := decodeTLCAction(Request{
		TraceVocabulary: verify.TraceVocabulary{Actions: map[string]string{"Action_schedule": "schedule"}},
	}, `Action_schedule line 10, col 1 to line 20, col 1 of module Umpire`)
	require.NoError(t, err)
	require.Equal(t, "schedule", action)
	require.Nil(t, bindings)
}

func TestITFStateDecoderRejectsUnlistedStateVariables(t *testing.T) {
	request := runnerRelationDecoderRequest()
	_, err := decodeITFTrace(request, `{
  "#meta": {"format": "ITF"},
  "vars": ["exists_source", "exists_target", "relation_link"],
  "states": [{
    "exists_source": {"#set": ["source#0"]},
    "exists_target": {"#set": ["target#0"]},
    "relation_link": {"#set": []},
    "unlisted": true
  }]
}`)
	require.ErrorContains(t, err, "unmapped state variable")
}

func TestIvyTraceDecoderMapsRelationValuations(t *testing.T) {
	request := runnerRelationDecoderRequest()
	request.TraceVocabulary.Identities = map[string]string{
		"source_0": "source#0",
		"target_0": "target#0",
	}

	evidence, err := decodeIvyTrace(request, `Trace follows...
********************************************************************************
[
    exists_source(source_0) = true
    exists_target(target_0) = true
    relation_link(source_0,target_0) = true
]`)
	require.NoError(t, err)
	require.Equal(t, []verify.RelationTuple{{Source: "source#0", Target: "target#0"}}, evidence.Initial.Relations["link"])
}

func TestIvyTraceDecoderReadsSmallModelActionBindings(t *testing.T) {
	request := runnerIvyCounterexampleRequest()
	evidence, err := decodeIvyTrace(request, `searching for a small model... done
[
    exists_nexusoperation(0) = false
]
call schedule
{
    [
        fml:op = 0
    ]
}`)

	require.NoError(t, err)
	require.Equal(t, []verify.ObservedTraceStep{{
		Action: "schedule", Bindings: verify.Bindings{"op": "NexusOperation#0"},
	}}, evidence.Steps)
}

func TestClassifyRejectsMalformedNativeStateEvidence(t *testing.T) {
	tests := []struct {
		name      string
		request   Request
		execution execution
		category  string
	}{
		{
			name: "TLC missing state variables",
			request: Request{
				Backend: TLC, Model: runnerCounterexampleModel(),
				TraceVocabulary: verify.TraceVocabulary{
					Properties:   map[string][]string{"terminal-link": {"terminal-link"}},
					EntityExists: map[string]string{"exists_NexusOperation": "NexusOperation"},
					EntityStates: map[string]string{"state_NexusOperation": "NexusOperation"},
				},
			},
			execution: execution{output: "State 1: <Initial predicate>\nError: Invariant terminal-link is violated.", err: errors.New("exit status 12")},
			category:  "native-trace-malformed",
		},
		{
			name:    "Ivy missing trace",
			request: runnerIvyCounterexampleRequest(),
			execution: execution{
				output: "Umpire.ivy: line 20: terminal_link ... FAIL\nerror: failed checks: 1",
				err:    errors.New("exit status 1"),
			},
			category: "native-trace-missing",
		},
		{
			name: "Apalache malformed JSON",
			request: Request{
				Backend: Apalache, Model: runnerCounterexampleModel(),
				TraceVocabulary: verify.TraceVocabulary{
					Properties: map[string][]string{"Safety": {"terminal-link"}},
				},
			},
			execution: execution{
				output: "Error: Invariant Safety is violated.", nativeTrace: "{", err: errors.New("exit status 1"),
			},
			category: "native-trace-malformed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := classify(test.request, test.execution)
			require.Equal(t, verify.Inconclusive, result.Status)
			require.Equal(t, verify.EvidenceFailure, result.Termination)
			require.Contains(t, result.Diagnostic, test.category)
		})
	}
}

func runnerRelationDecoderRequest() Request {
	return Request{
		Model: verify.Model{
			Version: "relation-decoder-test/v1",
			Entities: []verify.EntityType{
				{Name: "source", IDs: []string{"source#0"}},
				{Name: "target", IDs: []string{"target#0"}},
			},
			Relations: []verify.Relation{{Name: "link", Source: "source", Target: "target"}},
		},
		TraceVocabulary: verify.TraceVocabulary{
			EntityExists: map[string]string{"exists_source": "source", "exists_target": "target"},
			Relations:    map[string]string{"relation_link": "link"},
		},
	}
}
