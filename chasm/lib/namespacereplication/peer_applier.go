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

// PeerApplyResult is the transport-neutral terminal, non-error outcome of
// processing a namespace mutation at a peer cell. Failures are returned as
// errors and classified by the task handler (see classifyPeerErr), not modeled
// here.
type PeerApplyResult int

// PeerApplyRequest carries one transport attempt and its correlation metadata.
type PeerApplyRequest struct {
	SourceCluster       string
	TargetCluster       string
	ComponentBusinessID string
	ComponentRunID      string
	AttemptCount        int32
	Operation           enumsspb.NamespaceOperation
	Detail              *persistencespb.NamespaceDetail
	Shadow              bool
}

const (
	// PeerApplyResultUnspecified is invalid and must never be treated as a
	// successful peer apply.
	PeerApplyResultUnspecified PeerApplyResult = iota
	// PeerApplyResultApplied means the peer accepted the mutation as new state
	// (created or updated). Collapses the admin RPC's Applied / Created / Duplicate.
	PeerApplyResultApplied
	// PeerApplyResultShadowMatch means the peer processed a shadow request without
	// writing receiver state and the received payload matched its fingerprint.
	PeerApplyResultShadowMatch
	// PeerApplyResultShadowMismatch means the peer processed a shadow request
	// without writing receiver state and the received payload did not match its
	// fingerprint.
	PeerApplyResultShadowMismatch
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
// an alternative PeerApplier via fx.Decorate in the history service options —
// without touching any policy in this package.
type PeerApplier interface {
	Apply(
		ctx context.Context,
		request PeerApplyRequest,
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
	request PeerApplyRequest,
) (PeerApplyResult, error) {
	adminClient, err := a.clientBean.GetRemoteAdminClient(request.TargetCluster)
	if err != nil {
		return PeerApplyResultUnspecified, err
	}
	namespaceTask := nsreplication.NamespaceDetailToTaskAttributes(request.Operation, request.Detail)
	fingerprint, err := nsreplication.NamespaceTaskFingerprint(namespaceTask)
	if err != nil {
		return 0, fmt.Errorf("fingerprint namespace mutation: %w", err)
	}
	resp, err := adminClient.ApplyNamespaceMutation(ctx, &adminservice.ApplyNamespaceMutationRequest{
		NamespaceTask:       namespaceTask,
		Shadow:              request.Shadow,
		Fingerprint:         fingerprint,
		SourceCluster:       request.SourceCluster,
		ComponentBusinessId: request.ComponentBusinessID,
		ComponentRunId:      request.ComponentRunID,
		AttemptCount:        request.AttemptCount,
	})
	if err != nil {
		return PeerApplyResultUnspecified, err
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
		if request.Shadow {
			return PeerApplyResultUnspecified, unexpectedPeerOutcome(request.TargetCluster, request.Shadow, resp.GetOutcome())
		}
		return PeerApplyResultApplied, nil
	case adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH:
		if !request.Shadow {
			return PeerApplyResultUnspecified, unexpectedPeerOutcome(request.TargetCluster, request.Shadow, resp.GetOutcome())
		}
		return PeerApplyResultShadowMatch, nil
	case adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH:
		if !request.Shadow {
			return PeerApplyResultUnspecified, unexpectedPeerOutcome(request.TargetCluster, request.Shadow, resp.GetOutcome())
		}
		return PeerApplyResultShadowMismatch, nil
	case adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE:
		if request.Shadow {
			return PeerApplyResultUnspecified, unexpectedPeerOutcome(request.TargetCluster, request.Shadow, resp.GetOutcome())
		}
		return PeerApplyResultNoOpStale, nil
	case adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED:
		if request.Shadow {
			return PeerApplyResultUnspecified, unexpectedPeerOutcome(request.TargetCluster, request.Shadow, resp.GetOutcome())
		}
		return PeerApplyResultNotAdmitted, nil
	default:
		return PeerApplyResultUnspecified, unexpectedPeerOutcome(request.TargetCluster, request.Shadow, resp.GetOutcome())
	}
}

func unexpectedPeerOutcome(
	targetCell string,
	shadow bool,
	outcome adminservice.ApplyNamespaceMutationResponse_Outcome,
) error {
	return serviceerror.NewInternal(
		fmt.Sprintf("peer %s returned unexpected outcome %v for shadow=%t", targetCell, outcome, shadow))
}
