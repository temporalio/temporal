package faultinjection_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/rpc/faultinjection"
)

type scopedRequest struct {
	scope faultinjection.Scope
}

func (r scopedRequest) FaultScope() faultinjection.Scope { return r.scope }

func TestScopeFromRequest(t *testing.T) {
	t.Parallel()

	bothNamespaces := faultinjection.Scope{NamespaceID: "namespace-id", NamespaceName: "namespace-name"}
	for _, test := range []struct {
		name    string
		request any
		want    faultinjection.Scope
	}{
		{name: "no namespace", request: "request", want: faultinjection.Scope{}},
		{name: "namespace ID", request: &matchingservice.AddWorkflowTaskRequest{NamespaceId: "namespace-id"}, want: faultinjection.Scope{NamespaceID: "namespace-id"}},
		{name: "namespace name", request: &workflowservice.UpdateNamespaceRequest{Namespace: "namespace-name"}, want: faultinjection.Scope{NamespaceName: "namespace-name"}},
		{name: "namespace ID and name", request: &matchingservice.ListTaskQueuePartitionsRequest{NamespaceId: "namespace-id", Namespace: "namespace-name"}, want: bothNamespaces},
		{name: "explicit fault scope", request: scopedRequest{scope: bothNamespaces}, want: bothNamespaces},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, faultinjection.ScopeFromRequest(test.request))
		})
	}
}

func TestReplacement_Discards(t *testing.T) {
	t.Parallel()

	var r faultinjection.Replacement[string]
	require.Empty(t, r.Replaced())
	r.Drop()
	require.Empty(t, r.Discarded(), "nothing to drop")

	r.Replace("a")
	r.Replace("b")
	r.Replace("c")
	require.Equal(t, "c", r.Replaced())
	require.Equal(t, []string{"a", "b"}, r.Discarded())
	require.Empty(t, r.Discarded(), "discarded responses are taken once")

	r.Drop()
	require.Empty(t, r.Replaced())
	require.Equal(t, []string{"c"}, r.Discarded())

	r.Discard("x", "y")
	require.Equal(t, []string{"x", "y"}, r.Discarded())
}
