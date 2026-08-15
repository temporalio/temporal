package model

import (
	"context"
	"iter"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

const CallbackType = fact.CallbackType

var _ umpire.Entity = (*Callback)(nil)

// CallbackDeliveryResponse is the first accepted normalized response for one delivery.
type CallbackDeliveryResponse struct {
	DeliveryID  string
	Kind        string
	Fingerprint string
	ObservedAt  time.Time
}

// Callback is a non-lifecycled, non-secret routing identity shared by caller and handler observations.
type Callback struct {
	NamespaceID              string
	CallbackID               string
	OperationID              string
	OperationRunID           string
	OperationRequestID       string
	HandlerWorkflowID        string
	HandlerRunID             string
	HandlerWorkflowStartTime time.Time
	AttachmentEventTime      time.Time
	AttachmentEventID        int64
	ReferenceKind            string
	ReferenceValue           string
	ReferencedEventType      enumspb.EventType
	ResponseKind             string
	FirstResponseTime        time.Time
	DeliveryResponses        map[string]CallbackDeliveryResponse
	ConflictingResponses     []CallbackDeliveryResponse
	Malformed                bool
	ErrorClass               string
}

func NewCallback() *Callback {
	return &Callback{DeliveryResponses: make(map[string]CallbackDeliveryResponse)}
}

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
			setTimeIfZero(&c.HandlerWorkflowStartTime, value.HandlerWorkflowStartTime)
			setTimeIfZero(&c.AttachmentEventTime, value.AttachmentEventTime)
			if c.AttachmentEventID == 0 {
				c.AttachmentEventID = value.AttachmentEventID
			}
			setIfEmpty(&c.ReferenceKind, value.ReferenceKind)
			setIfEmpty(&c.ReferenceValue, value.ReferenceValue)
			if c.ReferencedEventType == enumspb.EVENT_TYPE_UNSPECIFIED {
				c.ReferencedEventType = value.ReferencedEventType
			}
			c.retainMalformed(value.Malformed, value.ErrorClass)
		case *fact.NexusStartResponse:
			setIfEmpty(&c.NamespaceID, value.NamespaceID)
			setIfEmpty(&c.CallbackID, value.CallbackID)
			c.retainResponse(value)
		default:
			continue
		}
	}
	return nil
}

func (c *Callback) retainResponse(observed *fact.NexusStartResponse) {
	if observed.DeliveryID == "" || observed.ResponseFingerprint == "" {
		return
	}
	if c.DeliveryResponses == nil {
		c.DeliveryResponses = make(map[string]CallbackDeliveryResponse)
	}
	response := CallbackDeliveryResponse{
		DeliveryID:  observed.DeliveryID,
		Kind:        observed.ResponseKind,
		Fingerprint: observed.ResponseFingerprint,
		ObservedAt:  observed.ObservedAt,
	}
	accepted, exists := c.DeliveryResponses[observed.DeliveryID]
	if exists {
		if accepted.Kind == response.Kind && accepted.Fingerprint == response.Fingerprint {
			return
		}
		for _, conflict := range c.ConflictingResponses {
			if conflict.DeliveryID == response.DeliveryID && conflict.Kind == response.Kind && conflict.Fingerprint == response.Fingerprint {
				return
			}
		}
		c.ConflictingResponses = append(c.ConflictingResponses, response)
		return
	}
	c.DeliveryResponses[observed.DeliveryID] = response
	if c.ResponseKind == "" {
		c.ResponseKind = observed.ResponseKind
		c.FirstResponseTime = observed.ObservedAt
	}
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

func setTimeIfZero(target *time.Time, value time.Time) {
	if target.IsZero() {
		*target = value
	}
}
