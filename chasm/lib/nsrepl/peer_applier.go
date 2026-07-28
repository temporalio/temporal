package nsrepl

import (
	"context"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	serverclient "go.temporal.io/server/client"
	"go.temporal.io/server/common/namespace/nsreplication"
)

// PeerApplyResult is the transport-neutral success outcome of applying a namespace
// mutation to a peer cell. Failures are returned as errors and classified by the
// task handler (see classifyPeerErr), not modeled here.
type PeerApplyResult int

const (
	// PeerApplyResultApplied means the peer accepted the mutation as new state
	// (created or updated). Collapses the admin RPC's Applied / Created / Duplicate.
	PeerApplyResultApplied PeerApplyResult = iota
	// PeerApplyResultNoOpStale means the peer already held equal-or-newer state
	// (apply-if-higher no-op). A success, not a failure.
	PeerApplyResultNoOpStale
)

// PeerApplier applies a committed namespace mutation to a single peer cell and
// reports the outcome. It is the transport seam of the peer fan-out: the retry,
// gating, state-machine, and observability policy all live in applyPeerTaskHandler,
// which calls Apply once per attempt and classifies any returned error.
//
// The default OSS implementation (adminClientPeerApplier) uses the cross-cluster
// ApplyNamespaceMutation admin RPC. A deployment that needs a different peer
// transport (e.g. calling UpdateNamespace directly on the peer host) can provide
// an alternative PeerApplier via fx — e.g. fx.Decorate/fx.Provide in the history
// service options — without touching any policy in this package.
type PeerApplier interface {
	Apply(
		ctx context.Context,
		targetCell string,
		operation enumsspb.NamespaceOperation,
		detail *persistencespb.NamespaceDetail,
	) (PeerApplyResult, error)
}

// adminClientPeerApplier is the default PeerApplier: it sends the full namespace
// snapshot to the peer cell's ApplyNamespaceMutation admin RPC, reusing the same
// NamespaceTaskAttributes wire shape and apply-if-higher receiver logic as the
// legacy queue transport.
type adminClientPeerApplier struct {
	clientBean serverclient.Bean
}

func newAdminClientPeerApplier(clientBean serverclient.Bean) PeerApplier {
	return &adminClientPeerApplier{clientBean: clientBean}
}

func (a *adminClientPeerApplier) Apply(
	ctx context.Context,
	targetCell string,
	operation enumsspb.NamespaceOperation,
	detail *persistencespb.NamespaceDetail,
) (PeerApplyResult, error) {
	adminClient, err := a.clientBean.GetRemoteAdminClient(targetCell)
	if err != nil {
		return PeerApplyResultApplied, err
	}
	resp, err := adminClient.ApplyNamespaceMutation(ctx, &adminservice.ApplyNamespaceMutationRequest{
		NamespaceTask: nsreplication.NamespaceDetailToTaskAttributes(operation, detail),
	})
	if err != nil {
		return PeerApplyResultApplied, err
	}
	if resp.GetOutcome() == adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE {
		return PeerApplyResultNoOpStale, nil
	}
	return PeerApplyResultApplied, nil
}
