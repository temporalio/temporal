package chasm

import (
	"encoding/base64"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonnexus "go.temporal.io/server/common/nexus"
)

const (
	// Base URL for Nexus->CHASM callbacks.
	NexusCompletionHandlerURL = "temporal://internal"
)

// NexusCompletionHandler is implemented by CHASM components that want to handle
// Nexus operation completion callbacks.
type NexusCompletionHandler interface {
	HandleNexusCompletion(MutableContext, *persistencespb.ChasmNexusCompletion) error
}

// GetNexusCallback generates a Callback message indicating a CHASM component
// to receive Nexus operation completion callbacks. Particularly useful for
// components that want to track a workflow start with StartWorkflowExecution.
func GetNexusCallback(ctx Context, component Component) (*commonpb.Callback, error) {
	if _, ok := component.(NexusCompletionHandler); !ok {
		return nil, errors.New("component must implement NexusCompletionHandler")
	}

	ref, err := ctx.Ref(component)
	if err != nil {
		return nil, err
	}

	encodedRef := base64.RawURLEncoding.EncodeToString(ref)
	headers := map[string]string{
		commonnexus.CallbackTokenHeader: encodedRef,
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
