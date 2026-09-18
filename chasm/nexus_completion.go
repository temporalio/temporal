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

// GenerateNexusCallback builds a Nexus completion callback targeting the CHASM component identified by
// serializedRef (obtained from Context.Ref). When encodeToken is true, the request ID is packed into
// the callback token (a NexusOperationCompletion envelope), so the completion is matched by a request
// ID that rides in the callback header and survives continue-as-new, rather than one read from mutable
// state. When encodeToken is false, the legacy format is emitted: the token is the bare base64-encoded
// ChasmComponentRef with no request ID. The caller chooses encodeToken (e.g. gated behind dynamic
// config) to keep the envelope format off the wire until the whole fleet can read it. Either format is
// always decodable by UnpackNexusCallbackToken.
func GenerateNexusCallback(serializedRef []byte, requestID string, encodeToken bool) (*commonpb.Callback, error) {
	var token string
	if encodeToken {
		var err error
		token, err = packNexusCallbackToken(serializedRef, requestID)
		if err != nil {
			return nil, err
		}
	} else {
		// Legacy format: the token is the bare base64-encoded ChasmComponentRef.
		token = base64.RawURLEncoding.EncodeToString(serializedRef)
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

// packNexusCallbackToken encodes a CHASM component ref and request ID into a callback token.
func packNexusCallbackToken(componentRef []byte, requestID string) (string, error) {
	b, err := proto.Marshal(&tokenspb.NexusOperationCompletion{
		ComponentRef: componentRef,
		RequestId:    requestID,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// UnpackNexusCallbackToken decodes a callback token produced by GenerateNexusCallback, returning the
// component ref and request ID. It accepts both token formats regardless of how the token was written:
// the NexusOperationCompletion envelope, and (for backward compatibility) the legacy bare base64-encoded
// ChasmComponentRef, in which case the request ID is empty.
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
