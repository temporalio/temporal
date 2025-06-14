// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package scheduler

import (
	"fmt"
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// The Backfiller sub state machine is responsible for buffering manually
	// requested actions. Each backfill request has its own Backfiller node.
	Backfiller struct {
		*schedulespb.BackfillerInternal
	}

	// The machine definition provides serialization/deserialization and type information.
	backfillerMachineDefinition struct{}

	BackfillerMachineState int
)

const (
	// Unique identifier for the Backfiller sub state machine.
	BackfillerMachineType = "scheduler.Backfiller"

	// The Backfiller has only a single running state.
	BackfillerMachineStateRunning BackfillerMachineState = 0
)

// The type of Backfill represented by an invididual Backfiller.
type BackfillRequestType int

const (
	RequestTypeTrigger BackfillRequestType = iota
	RequestTypeBackfill
)

var (
	_ hsm.StateMachine[BackfillerMachineState] = Backfiller{}
	_ hsm.StateMachineDefinition               = &backfillerMachineDefinition{}
)

func BackfillerMachineKey(id string) hsm.Key {
	return hsm.Key{
		Type: BackfillerMachineType,
		ID:   id,
	}
}

// MachineCollection creates a new typed [statemachines.Collection] for operations.
func BackfillerCollection(tree *hsm.Node) hsm.Collection[Backfiller] {
	return hsm.NewCollection[Backfiller](tree, BackfillerMachineType)
}

func (b Backfiller) State() BackfillerMachineState {
	return BackfillerMachineStateRunning
}

func (b Backfiller) SetState(_ BackfillerMachineState) {}

func (b Backfiller) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return b.tasks()
}

func (backfillerMachineDefinition) Type() string {
	return BackfillerMachineType
}

func (backfillerMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Backfiller); ok {
		return proto.Marshal(state.BackfillerInternal)
	}
	return nil, fmt.Errorf("invalid backfiller state provided: %v", state)
}

func (backfillerMachineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedulespb.BackfillerInternal{}
	return Backfiller{state}, proto.Unmarshal(body, state)
}

func (backfillerMachineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Backfiller") //nolint:forbidigo // this code is being immediately moved to CHASM
}

func (b Backfiller) RequestType() BackfillRequestType {
	if b.GetTriggerRequest() != nil {
		return RequestTypeTrigger
	}

	return RequestTypeBackfill
}

type backfillProgressResult struct {
	// BufferedStarts that should be enqueued to the Invoker.
	BufferedStarts []*schedulespb.BufferedStart

	// When the next batch of backfills should attempt buffering.
	NextInvocationTime time.Time

	// High water mark for when state was last updated.
	LastProcessedTime time.Time

	// When true, the backfill has completed and the node can be deleted.
	Complete bool
}

// Fired when the backfiller has made progress.
type EventBackfillProgress struct {
	Node *hsm.Node

	backfillProgressResult
}

// Applied when the Backfiller has made partial progress on its request and needs
// to retry.
var TransitionBackfillProgress = hsm.NewTransition(
	[]BackfillerMachineState{BackfillerMachineStateRunning},
	BackfillerMachineStateRunning,
	func(b Backfiller, event EventBackfillProgress) (hsm.TransitionOutput, error) {
		b.LastProcessedTime = timestamppb.New(event.LastProcessedTime)
		b.NextInvocationTime = timestamppb.New(event.NextInvocationTime)
		b.Attempt++
		return b.output()
	},
)
