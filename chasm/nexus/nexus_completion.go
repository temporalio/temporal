package nexus

import (
	"encoding/base64"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/nexus"
)

const (
	// Base URL for Nexus->CHASM callbacks.
	CompletionHandlerURL = "temporal://internal"
)

// NexusCompletionHandler is implemented by CHASM components that want to handle
// Nexus operation completion callbacks.
type CompletionHandler interface {
	HandleNexusCompletion(ctx chasm.MutableContext, completion *persistencespb.ChasmNexusCompletion) error
}

// GetCallback generates a Callback message indicating a CHASM component
// to receive Nexus operation completion callbacks. Particularly useful for
// components that want to track a workflow start with StartWorkflowExecution.
func GetCallback(ctx chasm.Context, component chasm.Component) (*commonpb.Callback, error) {
	if _, ok := component.(CompletionHandler); !ok {
		return nil, errors.New("component must implement HandleNexusCompletion")
	}

	ref, err := ctx.Ref(component)
	if err != nil {
		return nil, err
	}

	encodedRef := base64.RawURLEncoding.EncodeToString(ref)
	headers := map[string]string{
		nexus.CallbackTokenHeader: encodedRef,
	}

	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    CompletionHandlerURL,
				Header: headers,
			},
		},
	}, nil
}
