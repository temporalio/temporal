package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace/nsreplication"
	"google.golang.org/protobuf/proto"
)

type testNamespaceMutationExecutor struct {
	outcome nsreplication.ApplyOutcome
	err     error
	task    *replicationspb.NamespaceTaskAttributes
}

func (e *testNamespaceMutationExecutor) ExecuteWithOutcome(_ context.Context, task *replicationspb.NamespaceTaskAttributes) (nsreplication.ApplyOutcome, error) {
	e.task = task
	return e.outcome, e.err
}

func TestAuthoritativeNamespaceMutationUsesDecodedPayload(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome nsreplication.ApplyOutcome
		want    adminservice.ApplyNamespaceMutationResponse_Outcome
		err     error
	}{
		{name: "created", outcome: nsreplication.ApplyOutcomeCreated, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED},
		{name: "applied", outcome: nsreplication.ApplyOutcomeApplied, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED},
		{name: "stale", outcome: nsreplication.ApplyOutcomeNoOpStale, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE},
		{name: "duplicate", outcome: nsreplication.ApplyOutcomeDuplicate, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE},
		{name: "not admitted", outcome: nsreplication.ApplyOutcomeNotAdmitted, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED},
		{name: "executor failure", err: serviceerror.NewUnavailable("store unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			executor := &testNamespaceMutationExecutor{outcome: tc.outcome, err: tc.err}
			handler := &AdminHandler{logger: log.NewNoopLogger(), namespaceMutationExecutor: executor}
			task := shadowObservationTask()
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			response, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{
				NamespaceTaskPayload:    payload,
				NamespaceTask:           &replicationspb.NamespaceTaskAttributes{Id: "ignored-typed-task"},
				ShadowLegacyTaskPayload: []byte("ignored-shadow-reference"),
			})
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, response.GetOutcome())
			}
			require.True(t, proto.Equal(task, executor.task))
		})
	}
}

func TestAuthoritativeNamespaceMutationRejectsMalformedPayload(t *testing.T) {
	t.Parallel()
	executor := &testNamespaceMutationExecutor{}
	handler := &AdminHandler{logger: log.NewNoopLogger(), namespaceMutationExecutor: executor}
	_, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: []byte{0xff}})
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)
	require.Nil(t, executor.task)
}
