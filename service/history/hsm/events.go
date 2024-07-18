// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package hsm

import (
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

// ErrNotCherryPickable should be returned by CherryPick if an event should not be cherry picked for whatever reason.
var ErrNotCherryPickable = errors.New("event not cherry pickable")

// EventDefinition is a definition for a history event for a given event type.
type EventDefinition interface {
	Type() enumspb.EventType
	// IsWorkflowTaskTrigger returns a boolean indicating whether this event type should trigger a workflow task.
	IsWorkflowTaskTrigger() bool
	// Apply a history event to the state machine. Triggered during replication and workflow reset.
	Apply(root *Node, event *historypb.HistoryEvent) error
	// Cherry pick (a.k.a "reapply") an event from a different history branch.
	// Implementations should apply the event to the machine state and return nil in case the event is cherry-pickable.
	// Command events should never be cherry picked as we rely on the workflow to reschedule them.
	// Return [ErrNotCherryPickable], [ErrStateMachineNotFound], or [ErrInvalidTransition] to skip cherry picking. Any
	// other error is considered fatal and will abort the cherry pick process.
	CherryPick(root *Node, event *historypb.HistoryEvent) error
}
