package namespacereplication

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
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
	// PeerApplyResultNotAdmitted means the peer's admission policy declined the
	// namespace (receiver returned OUTCOME_NOT_ADMITTED). Terminal and not a
	// failure — the peer chose not to hold this namespace, so there is nothing to
	// retry. Kept distinct from Applied so we never record a phantom write.
	PeerApplyResultNotAdmitted
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
		return 0, err
	}
	resp, err := adminClient.ApplyNamespaceMutation(ctx, &adminservice.ApplyNamespaceMutationRequest{
		NamespaceTask: nsreplication.NamespaceDetailToTaskAttributes(operation, detail),
	})
	if err != nil {
		return 0, err
	}
	// Map the receiver's wire outcome to a transport-neutral result. Exhaustive on
	// purpose: adding a wire outcome must force a decision here rather than being
	// silently absorbed into Applied. Applied / Created / Duplicate all mean "the
	// peer now holds our state"; a success response we can't classify is a protocol
	// violation and is surfaced as an error (so the handler retries/logs it) rather
	// than recorded as a phantom write.
	switch resp.GetOutcome() {
	case adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE:
		return PeerApplyResultApplied, nil
	case adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE:
		return PeerApplyResultNoOpStale, nil
	case adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED:
		return PeerApplyResultNotAdmitted, nil
	default:
		return 0, serviceerror.NewInternal(
			fmt.Sprintf("peer %s returned unexpected apply outcome %v", targetCell, resp.GetOutcome()))
	}
}
