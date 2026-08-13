package model

import (
	"context"
	"iter"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

const CallbackType = fact.CallbackType

var _ umpire.Entity = (*Callback)(nil)

// Callback is a non-lifecycled, non-secret routing identity shared by caller and handler observations.
type Callback struct {
	NamespaceID        string
	CallbackID         string
	OperationID        string
	OperationRunID     string
	OperationRequestID string
	HandlerWorkflowID  string
	HandlerRunID       string
	Malformed          bool
	ErrorClass         string
}

func NewCallback() *Callback { return &Callback{} }

func (*Callback) Type() umpire.EntityType { return CallbackType }

func (c *Callback) OnFact(_ context.Context, _ *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	for observed := range facts {
		switch value := observed.(type) {
		case *fact.NexusCallbackObservation:
			setIfEmpty(&c.NamespaceID, value.NamespaceID)
			setIfEmpty(&c.CallbackID, value.CallbackID)
			setIfEmpty(&c.OperationID, value.OperationID)
			setIfEmpty(&c.OperationRunID, value.OperationRunID)
			setIfEmpty(&c.OperationRequestID, value.OperationRequestID)
			c.retainMalformed(value.Malformed, value.ErrorClass)
		case *fact.WorkflowCallbackAttachment:
			setIfEmpty(&c.NamespaceID, value.NamespaceID)
			setIfEmpty(&c.CallbackID, value.CallbackID)
			setIfEmpty(&c.HandlerWorkflowID, value.HandlerWorkflowID)
			setIfEmpty(&c.HandlerRunID, value.HandlerRunID)
			c.retainMalformed(value.Malformed, value.ErrorClass)
		default:
			continue
		}
	}
	return nil
}

func (c *Callback) retainMalformed(malformed bool, errorClass string) {
	if !malformed {
		return
	}
	c.Malformed = true
	setIfEmpty(&c.ErrorClass, errorClass)
}

func setIfEmpty(target *string, value string) {
	if *target == "" {
		*target = value
	}
}
