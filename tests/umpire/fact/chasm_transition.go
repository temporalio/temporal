package fact

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// ChasmTransition is the single, generic span fact decoded from the CHASM engine's
// `chasm.transition` event (emitted by chasm.Transition.Apply under
// TEMPORAL_OTEL_DEBUG). One event type covers every CHASM component; the component
// identity carried on the event (type, path, execution key) lets it be routed to a
// specific entity. Today it routes Nexus-operation transitions; other CHASM
// components can be added by extending route().
//
// Unlike the per-event span facts, its Name() is the OTEL event name, not the
// struct name, so it is registered only with the decoder (routing is by
// TargetEntity, not by RegisterFact subscription).
type ChasmTransition struct {
	ComponentType string
	ComponentPath string
	Source        string
	Destination   string
	NamespaceID   string
	WorkflowID    string
	RunID         string
	EntityPath    *umpire.EntityPath
}

func (e *ChasmTransition) Name() string { return telemetry.EventChasmTransition }

func (e *ChasmTransition) TargetEntity() *umpire.EntityPath { return e.EntityPath }

func (e *ChasmTransition) ImportSpanEvent(attrs attribute.Set) bool {
	e.ComponentType = strAttr(attrs, telemetry.AttrChasmComponentType)
	e.ComponentPath = strAttr(attrs, telemetry.AttrChasmComponentPath)
	e.Source = strAttr(attrs, telemetry.AttrChasmTransitionSource)
	e.Destination = strAttr(attrs, telemetry.AttrChasmTransitionDestination)
	e.NamespaceID = strAttr(attrs, telemetry.AttrNamespaceID)
	e.WorkflowID = strAttr(attrs, telemetry.AttrWorkflowID)
	e.RunID = strAttr(attrs, telemetry.AttrRunID)
	e.EntityPath = e.route()
	return e.EntityPath != nil
}

// IsNexusOperation reports whether this transition belongs to a Nexus operation.
func (e *ChasmTransition) IsNexusOperation() bool {
	return strings.HasSuffix(e.ComponentType, "nexusoperation.Operation")
}

// route maps the CHASM component identity to a umpire entity path, or nil when the
// component is not (yet) modelled. A Nexus operation is keyed "<workflowID>:<path>"
// under its caller workflow.
//
// A transition with an empty ComponentPath is ignored: the component is created in
// the current CHASM transition (e.g. the scheduling transition fires before the
// operation is attached to the tree, so structuredRef cannot yet resolve its path),
// leaving no per-operation identity to route on. Such creation-time transitions are
// immediately superseded by path-bearing transitions on the same operation, which
// the Monitor's legal forward-jumps carry to the true observed state — so ignoring
// them avoids minting a spurious, un-progressing second entity.
func (e *ChasmTransition) route() *umpire.EntityPath {
	if !e.IsNexusOperation() || e.WorkflowID == "" || e.ComponentPath == "" {
		return nil
	}
	self := umpire.NewEntityID(NexusOperationType, e.WorkflowID+":"+e.ComponentPath)
	parents := []umpire.EntityID{umpire.NewEntityID(WorkflowType, e.WorkflowID)}
	return nsPath(e.NamespaceID, self, parents...)
}

func strAttr(attrs attribute.Set, key attribute.Key) string {
	if v, ok := attrs.Value(key); ok {
		return v.AsString()
	}
	return ""
}
