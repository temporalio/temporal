package nsrepl

import (
	"context"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nsreplpb "go.temporal.io/server/chasm/lib/nsrepl/gen/nsreplpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
)

// handler implements the NamespaceReplicationService. It runs on history (where
// the CHASM engine is in context via ChasmEngineInterceptor) and is the entry
// point for triggering namespace mutations through the CHASM transport.
type handler struct {
	nsreplpb.UnimplementedNamespaceReplicationServiceServer

	logger log.Logger
}

func newHandler(logger log.Logger) *handler {
	return &handler{logger: logger}
}

// TriggerNamespaceMutation starts a NamespaceMutationComponent for the given
// namespace and waits for the local apply (ApplyLocalTask) to complete. Peer
// fan-out continues asynchronously after this returns.
//
// Each mutation gets its own unique-per-invocation BusinessID
// (`namespace_id:mutation_uuid`), so concurrent calls to the same namespace
// each create their own component. Serialization happens at the metadata
// store's version-CAS (matching legacy behavior); a CAS conflict surfaces to
// the caller as FailedPrecondition, and the caller re-issues UpdateNamespace
// with fresh state.
//
// Returns:
//   - On local-apply success: the new notification_version.
//   - On local-apply failure (CAS conflict, store unavailable, etc.): a gRPC
//     error mapped from the underlying cause.
func (h *handler) TriggerNamespaceMutation(
	ctx context.Context,
	req *nsreplpb.TriggerNamespaceMutationRequest,
) (response *nsreplpb.TriggerNamespaceMutationResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	if req == nil || req.GetMutation() == nil {
		return nil, serviceerror.NewInvalidArgument("mutation is required")
	}
	namespaceID := req.GetNamespaceId()
	if namespaceID == "" {
		return nil, serviceerror.NewInvalidArgument("namespace_id is required")
	}
	if req.GetMutation().GetNamespaceDetail() == nil {
		return nil, serviceerror.NewInvalidArgument("mutation.namespace_detail is required")
	}

	// BusinessID is unique per mutation invocation. Every UpdateNamespace call
	// results in its own component regardless of any prior in-flight mutation
	// for the same namespace. This avoids the "fail-fast if a component is
	// already running" behavior that would deviate from legacy semantics.
	// Concurrent same-namespace mutations serialize through the metadata store
	// CAS in ApplyLocalTask, matching how legacy UpdateNamespace serializes.
	businessID := namespaceID + ":" + uuid.NewString()
	key := chasm.ExecutionKey{
		NamespaceID: primitives.SystemNamespaceID,
		BusinessID:  businessID,
	}

	if _, startErr := chasm.StartExecution[*NamespaceMutationComponent, *nsreplpb.NamespaceMutation](
		ctx,
		key,
		func(mctx chasm.MutableContext, m *nsreplpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
			c := NewNamespaceMutationComponent(m)
			// Attach a Visibility child component so the parent shows up in
			// `temporal workflow list`. WithBusinessIDAlias on the registration
			// declares the alias; this field is what causes a visibility record
			// to actually be written for the execution.
			c.Visibility = chasm.NewComponentField(mctx, chasm.NewVisibility(mctx))
			// Fire the initial transition to schedule ApplyLocalTask. Without this
			// the component is created but no task is ever queued, and the
			// component sits idle until retention.
			if err := TransitionScheduleLocal.Apply(c, mctx, EventScheduleLocal{}); err != nil {
				return nil, err
			}
			return c, nil
		},
		req.GetMutation(),
	); startErr != nil {
		return nil, startErr
	}

	// Wait until ApplyLocalTask reaches a terminal outcome. PollComponent's
	// predicate is monotonic: once the local apply outcome moves out of PENDING,
	// it stays terminal.
	ref := chasm.NewComponentRef[*NamespaceMutationComponent](key)
	out, _, pollErr := chasm.PollComponent(
		ctx,
		ref,
		func(c *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (*nsreplpb.TriggerNamespaceMutationResponse, bool, error) {
			local := c.GetLocalApply()
			switch local.GetOutcome() {
			case nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED:
				return &nsreplpb.TriggerNamespaceMutationResponse{
					NewVersion: local.GetNewVersion(),
				}, true, nil
			case nsreplpb.LOCAL_APPLY_OUTCOME_FAILED:
				msg := "local apply failed"
				if f := local.GetFailure(); f != nil && f.GetMessage() != "" {
					msg = f.GetMessage()
				}
				return nil, true, serviceerror.NewFailedPrecondition(msg)
			default:
				// Still pending; keep waiting.
				return nil, false, nil
			}
		},
		nil,
	)
	if pollErr != nil {
		return nil, pollErr
	}
	return out, nil
}
