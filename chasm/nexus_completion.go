package chasm

import (
	"encoding/base64"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"google.golang.org/protobuf/proto"
)

// NexusCompletionHandlerURL is the user-visible URL for Nexus->CHASM callbacks.
const NexusCompletionHandlerURL = "temporal://internal"

// nexusCallbackTokenHeader is the callback header key carrying the completion token.
// NOTE: There's a constant defined for this in common/nexus but to avoid circular dependencies we
// redefine it here. nexus.Header lookups are case-insensitive, so this matches the canonical key.
const nexusCallbackTokenHeader = "temporal-callback-token"

// NexusCompletionHandler is implemented by CHASM components that want to handle Nexus operation completion callbacks.
type NexusCompletionHandler interface {
	HandleNexusCompletion(ctx MutableContext, completion *persistencespb.ChasmNexusCompletion) error
}

// NexusCompletionHandlerComponent is a CHASM [Component] that also implements [NexusCompletionHandler].
type NexusCompletionHandlerComponent interface {
	Component
	NexusCompletionHandler
}

// GenerateNexusCallback generates a Callback message indicating a CHASM component to receive Nexus operation completion
// callbacks. Particularly useful for components that want to track a workflow start with StartWorkflowExecution.
func GenerateNexusCallback(ctx Context, component NexusCompletionHandlerComponent) (*commonpb.Callback, error) {
	ref, err := ctx.Ref(component)
	if err != nil {
		return nil, err
	}

	token, err := PackNexusCallbackToken(ref, "")
	if err != nil {
		return nil, err
	}

	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    NexusCompletionHandlerURL,
				Header: map[string]string{nexusCallbackTokenHeader: token},
			},
		},
	}, nil
}

// PackNexusCallbackToken encodes a CHASM component ref and (optional) request ID into a callback
// token. The request ID travels in the callback header, so it survives continue-as-new with the
// callback itself - the delivering side reads it from the token rather than from any mutable state.
func PackNexusCallbackToken(componentRef []byte, requestID string) (string, error) {
	b, err := proto.Marshal(&tokenspb.NexusOperationCompletion{
		ComponentRef: componentRef,
		RequestId:    requestID,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// UnpackNexusCallbackToken decodes a callback token produced by PackNexusCallbackToken, returning the
// component ref and request ID. For backward compatibility it also accepts the legacy format where the
// token is the bare base64-encoded ChasmComponentRef (in which case the request ID is empty).
func UnpackNexusCallbackToken(encoded string) (componentRef []byte, requestID string, err error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, "", err
	}
	completion := &tokenspb.NexusOperationCompletion{}
	if proto.Unmarshal(raw, completion) == nil && len(completion.GetComponentRef()) > 0 &&
		proto.Unmarshal(completion.GetComponentRef(), &persistencespb.ChasmComponentRef{}) == nil {
		return completion.GetComponentRef(), completion.GetRequestId(), nil
	}
	// Legacy format: the raw bytes are the ChasmComponentRef directly.
	return raw, "", nil
}

// WithNexusCallbackRequestID returns a copy of the given CHASM Nexus callback with requestID packed
// into its token, so the request ID is preserved across continue-as-new. Non-Nexus callbacks are
// returned unchanged.
func WithNexusCallbackRequestID(cb *commonpb.Callback, requestID string) (*commonpb.Callback, error) {
	nexusCb := cb.GetNexus()
	if nexusCb == nil {
		return cb, nil
	}
	ref, _, err := UnpackNexusCallbackToken(nexusCb.GetHeader()[nexusCallbackTokenHeader])
	if err != nil {
		return nil, err
	}
	token, err := PackNexusCallbackToken(ref, requestID)
	if err != nil {
		return nil, err
	}
	out := proto.Clone(cb).(*commonpb.Callback) //nolint:revive // proto.Clone of *Callback is always *Callback
	out.GetNexus().GetHeader()[nexusCallbackTokenHeader] = token
	return out, nil
}
