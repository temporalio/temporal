package chasm

import (
	"encoding/base64"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// NexusCompletionHandlerURL is the user-visible URL for Nexus->CHASM callbacks.
const NexusCompletionHandlerURL = "temporal://internal"

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

	encodedRef := base64.RawURLEncoding.EncodeToString(ref)
	headers := map[string]string{
		// NOTE: There's a constant defined for this in common/nexus but to avoid circular dependencies we redefine it here.
		// This is acceptable since we are going to eventually have a strongly typed field for passing tokens around.
		"temporal-callback-token": encodedRef,
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
