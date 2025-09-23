package chasm

import (
	"encoding/base64"

	"github.com/nexus-rpc/sdk-go/nexus"
)

const (
	// Header name for the CHASM ComponentRef.
	NexusComponentRefHeader = "X-CHASM-Component-Ref"

	// Base URL for Nexus->CHASM callbacks.
	NexusCompletionHandlerBaseURL = "temporal://internal/chasm"
)

// NexusCompletionHandler is implemented by CHASM components that want to handle
// Nexus operation completion callbacks.
type NexusCompletionHandler interface {
	HandleNexusCompletion(MutableContext, nexus.OperationCompletion) error
}

// RegisterNexusCallback generates a callback URL and headers for a CHASM component
// to receive Nexus operation completion callbacks.
func RegisterNexusCallback(ctx Context, component Component) (string, map[string]string, error) {
	ref, err := ctx.Ref(component)
	if err != nil {
		return "", nil, err
	}

	encodedRef := base64.RawURLEncoding.EncodeToString(ref)
	headers := map[string]string{
		NexusComponentRefHeader: encodedRef,
	}

	return NexusCompletionHandlerBaseURL, headers, nil
}
