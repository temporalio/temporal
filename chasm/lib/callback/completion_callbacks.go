package callback

import (
	"fmt"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/callbacks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Completion callbacks are how most [Callback] components are used within CHASM. The types and
// functions within ths file are to unify the logic required to implement that that capability
// correctly.
//
// To have a CHASM component support completion callbacks, you would need to do the following:
//
// - The component must persist two fields: a chasm.Map[string, *Callback] and a
//   callbackspb.CallbackMetadata.
// - The component must implement the [Host] interface, providing a way to access that data.
//
// Then, the component would just need to use the methods for validating, attaching, and
// executing completion callbacks when appropriate.
//
// - [CheckLimts(...)] to enforce aggregate callback checks.
// - [Attach(...)] to mutate the [Host] compoment and update the relevant metadata.
// - [ScheduleCompletionCallbacks(...)] to invoke the callbacks.

// Holder is a component that has completion callbacks.
type Holder interface {
	// CompletionCallbacks returns the mapping of completion callbacks attached to the component.
	// The returned value may be mutated, so the implementer MUST initialize the backing store if
	// needed.
	CompletionCallbacks() chasm.Map[string, *Callback]
}

// Host is a component that supports completion callbacks.
type Host interface {
	Holder
	// CompletionSource is how a callback would obtain the value to be delivered via callback.
	CompletionSource

	LifecycleState(ctx chasm.Context) chasm.LifecycleState

	// CompletionCallbackMetadata returns a mutable pointer to the host's CallbackMetadata.
	// The returned value may be mutated, so the implementer MUST initialize the backing store if
	// needed.
	CompletionCallbackMetadata() *callbackspb.CallbackMetadata
}

// completionCallbackID defines the stable key used for keeping track of attached completion callbacks
// within a chasm.Map[string, *Callback]. To disambiguate the ID when multiple completion callbacks are
// attached in the same request, both the requestID and the index are required.
func completionCallbackID(requestID string, idx int) string {
	return fmt.Sprintf("%s-%d", requestID, idx)
}

// HasCallbacksForRequest returns true if the given completion callbacks map has any callbacks attached
// that came from the given request ID.
func HasCallbacksForRequest(ctx chasm.Context, holder Holder, requestID string) bool {
	if holder == nil {
		return false
	}
	// This relies on attachment being atomic. Assuming that if the first/0th completion callback is
	// present, then all the other callbacks for the same request would be present too.
	firstID := completionCallbackID(requestID, 0)
	cbs := holder.CompletionCallbacks()
	_, ok := cbs[firstID]
	return ok
}

// Usage describes the status of callbacks attached to a Host.
type Usage callbacks.CurrentCallbacksInfo

func UsageOf(m *callbackspb.CallbackMetadata) Usage {
	return Usage{
		Count:     int(m.GetTotalCallbacksCount()),
		TotalSize: int(m.GetTotalCallbacksSize()),
	}
}

// Reserve updates the Usage value to account for any callbacks that are not yet
// attached to the Host. (e.g. Workflow Updates that are being validated, and not
// yet been accepted.)
func (u *Usage) Reserve(cbs []*commonpb.Callback) {
	u.Count += len(cbs)
	for _, cb := range cbs {
		u.TotalSize += cb.Size()
	}
}

// ValidateAdditions enforces the aggregate callback validation checks.
func ValidateAdditions(
	namespaceName string,
	usage Usage,
	newCallbacks []*commonpb.Callback,
	validator callbacks.Validator,
) error {
	existing := callbacks.CurrentCallbacksInfo(usage)
	return validator.ValidateAdditions(namespaceName, newCallbacks, existing)
}

// AttachOption customizes how callbacks are attached.
type AttachOption func(*attachOptions)

type attachOptions struct {
	// If set, the requestID that added the completion callback will be also be used for the
	// Callback component itself.
	//
	// This is incorrect and is a bug. Reusing the originating request ID is ambiguous when
	// multiple callbacks are attached in the same request that are routed to the same
	// destination. This option is used to preserve the behavior for Workflows, which rely
	// on the request ID to resolve buffered workflow events.
	//
	// TODO(https://github.com/temporalio/temporal/issues/11958): Fix this.
	reuseRequestIDForCallback bool
}

// WithReusedRequestID makes each attached callback use the ID of the request that attached it,
// instead of its own unique request ID. See [attachOptions.reuseRequestIDForCallback].
func WithReusedRequestID() AttachOption {
	return func(o *attachOptions) {
		o.reuseRequestIDForCallback = true
	}
}

// ValidateAndAttach validates newCallbacks and attaches them to [host].
//
// The function is idempotent, and will be a no-op if the callbacks associated with [requestID] are
// already present on the host. This runs before the closed check, so a retry of a previous request
// will be successful even if the underlying component is in a terminal state.
//
// IMPORTANT: This function only performs "aggregate" callback validations. It is assumed that the
// [callback.Validator]'s Validate method has already been called to check the callbacks individually.
func ValidateAndAttach(
	ctx chasm.MutableContext,
	host Host,
	requestID string,
	registrationTime *timestamppb.Timestamp,
	newCallbacks []*commonpb.Callback,
	namespaceName string,
	validator callbacks.Validator,
	opts ...AttachOption,
) error {
	if len(newCallbacks) == 0 {
		return nil
	}
	if requestID == "" {
		return serviceerror.NewInvalidArgument("cannot attach completion callbacks without a request ID")
	}

	// Idempotency check.
	if HasCallbacksForRequest(ctx, host, requestID) {
		return nil
	}

	if host.LifecycleState(ctx).IsClosed() {
		return serviceerror.NewFailedPrecondition("cannot attach callbacks to a closed execution")
	}

	usage := UsageOf(host.CompletionCallbackMetadata())
	if err := ValidateAdditions(namespaceName, usage, newCallbacks, validator); err != nil {
		return err
	}
	return Attach(ctx, host, requestID, registrationTime, newCallbacks, opts...)
}

// Attach converts newCallbacks to CHASM callback components and adds them to the host's
// completion callback map, updating the CallbackMetadata as needed.
//
// This function does not perform any validation, so it safe to call when reapplying history
// events that were already accepted. (e.g. NDC replication, workflow reset, history import.)
//
// The function is idempotent, and will be a no-op if the callbacks associated with [requestID] are
// already present on the host.
func Attach(
	ctx chasm.MutableContext,
	host Host,
	requestID string,
	registrationTime *timestamppb.Timestamp,
	newCallbacks []*commonpb.Callback,
	opts ...AttachOption,
) error {
	if len(newCallbacks) == 0 {
		return nil
	}
	var options attachOptions
	for _, opt := range opts {
		opt(&options)
	}

	// Idempotency check.
	if HasCallbacksForRequest(ctx, host, requestID) {
		return nil
	}

	// Do the conversion from commonpb.Callback to Callback here, so that we fail
	// before we mutate the existing callbacks or callback metadata.
	chasmCBs := make([]*callbackspb.Callback, len(newCallbacks))
	for idx, cb := range newCallbacks {
		chasmCB, err := FromAPICallback(cb)
		if err != nil {
			return err
		}
		chasmCBs[idx] = chasmCB
	}

	// Assert the backing stores have been initialized.
	existingCallbacks := host.CompletionCallbacks()
	if existingCallbacks == nil {
		return serviceerror.NewInternal("field not initialized: completion callbacks")
	}
	cbMetadata := host.CompletionCallbackMetadata()
	if cbMetadata == nil {
		return serviceerror.NewInternal("field not initialized: callback metadata")
	}

	for idx, chasmCB := range chasmCBs {
		cbRequestID := uuid.NewString()
		// Opt-into the incorrect behavior of reusing the same request ID for the attached callback.
		// This is only needed for Workflow cases until we can address the underlying issue.
		if options.reuseRequestIDForCallback {
			cbRequestID = requestID
		}
		callbackObj := NewCallback(cbRequestID, registrationTime, chasmCB)
		existingCallbacks[completionCallbackID(requestID, idx)] = chasm.NewComponentField(ctx, callbackObj)

		cbMetadata.TotalCallbacksCount++
		cbMetadata.TotalCallbacksSize += int64(newCallbacks[idx].Size())
	}
	return nil
}

// ScheduleCompletionCallbacks transitions all of the [Holder]s completion callbacks
// state from STANDBY to SCHEDULED state, triggering their invocation.
//
// Must be called when the source [Host] is in a terminal state.
func ScheduleCompletionCallbacks(ctx chasm.MutableContext, holder Holder) error {
	for _, field := range holder.CompletionCallbacks() {
		cb := field.Get(ctx)
		if cb.Status != callbackspb.CALLBACK_STATUS_STANDBY {
			continue
		}
		if err := TransitionScheduled.Apply(cb, ctx, EventScheduled{}); err != nil {
			return err
		}
	}
	return nil
}
