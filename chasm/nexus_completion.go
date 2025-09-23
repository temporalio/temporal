package chasm

import (
	"encoding/base64"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
)

const (
	// Header name for the CHASM ComponentRef.
	NexusComponentRefHeader = "X-CHASM-Component-Ref"

	// Base URL for Nexus->CHASM callbacks.
	NexusCompletionHandlerURL = "temporal://internal/chasm"
)

// NexusCompletionHandler is implemented by CHASM components that want to handle
// Nexus operation completion callbacks.
type NexusCompletionHandler interface {
	HandleNexusCompletion(MutableContext, nexus.OperationCompletion) error
}

// GetNexusCallback generates a Callback message indicating a CHASM component
// to receive Nexus operation completion callbacks. Particularly useful for
// components that want to track a workflow start with StartWorkflowExecution.
func GetNexusCallback(ctx Context, component Component) (*commonpb.Callback, error) {
	ref, err := ctx.Ref(component)
	if err != nil {
		return nil, err
	}

	encodedRef := base64.RawURLEncoding.EncodeToString(ref)
	headers := map[string]string{
		NexusComponentRefHeader: encodedRef,
	}

	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    NexusCompletionHandlerURL,
				Header: headers,
			},
		},
	}, nil
}
