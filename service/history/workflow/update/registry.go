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

package update

import (
	"fmt"
	"sync"

	"github.com/gogo/protobuf/types"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
)

type (
	Registry interface {
		Add(request *updatepb.Request) (*Update, Duplicate, RemoveFunc)

		CreateOutgoingMessages(startedEventID int64) ([]*protocolpb.Message, error)

		HasPending(filterMessages []*protocolpb.Message) bool
		ValidateIncomingMessages(messages []*protocolpb.Message) error
		ProcessIncomingMessages(messages []*protocolpb.Message) error
	}

	Duplicate bool

	RemoveFunc func()

	RegistryImpl struct {
		sync.RWMutex
		updates map[string]*Update
	}
)

var _ Registry = (*RegistryImpl)(nil)

func NewRegistry() *RegistryImpl {
	return &RegistryImpl{
		updates: make(map[string]*Update),
	}
}

func (r *RegistryImpl) Add(request *updatepb.Request) (*Update, Duplicate, RemoveFunc) {
	r.Lock()
	defer r.Unlock()
	protocolInstanceID := request.GetMeta().GetUpdateId()
	upd, ok := r.updates[protocolInstanceID]
	if ok {
		return upd, true, nil
	}
	upd = newUpdate(request, protocolInstanceID)
	r.updates[upd.protocolInstanceID] = upd
	return upd, false, func() { r.remove(protocolInstanceID) }
}

func (r *RegistryImpl) HasPending(filterMessages []*protocolpb.Message) bool {
	// Filter out updates which will be accepted or rejected by current workflow task messages.
	// These updates have Pending state in the registry but in fact, they are already accepted or rejected by worker
	// and shouldn't be counted as Pending.
	notPendingUpdates := make(map[string]struct{})
	for _, message := range filterMessages {
		if types.Is(message.GetBody(), (*updatepb.Acceptance)(nil)) {
			notPendingUpdates[message.GetProtocolInstanceId()] = struct{}{}
		} else if types.Is(message.GetBody(), (*updatepb.Rejection)(nil)) {
			notPendingUpdates[message.GetProtocolInstanceId()] = struct{}{}
		}
	}

	r.RLock()
	defer r.RUnlock()

	for _, update := range r.updates {
		if _, notPending := notPendingUpdates[update.protocolInstanceID]; !notPending && update.state == statePending {
			return true
		}
	}
	return false
}

func (r *RegistryImpl) CreateOutgoingMessages(startedEventID int64) ([]*protocolpb.Message, error) {
	r.RLock()
	defer r.RUnlock()
	numPendingUpd := 0
	for _, upd := range r.updates {
		if upd.state == statePending {
			numPendingUpd++
		}
	}
	if numPendingUpd == 0 {
		return nil, nil
	}

	// TODO (alex-update): currently sequencing_id is simply pointing to the event before WorkflowTaskStartedEvent.
	//  SDKs are supposed to respect this and process messages (specifically, updates) after event with that ID.
	//  In the future, sequencing_id could point to some specific event (specifically, signal) after which the update should be processed.
	//  Currently, it is not possible due to buffered events reordering on server and events reordering in some SDKs.
	sequencingEventID := startedEventID - 1

	updMessages := make([]*protocolpb.Message, 0, numPendingUpd)
	for _, upd := range r.updates {
		if upd.state == statePending {
			messageBody, err := types.MarshalAny(upd.request)
			if err != nil {
				return nil, err
			}
			updMessages = append(updMessages, &protocolpb.Message{
				Id:                 upd.messageID,
				ProtocolInstanceId: upd.protocolInstanceID,
				SequencingId: &protocolpb.Message_EventId{
					EventId: sequencingEventID,
				},
				Body: messageBody,
			})
		}
	}
	return updMessages, nil
}

func (r *RegistryImpl) ValidateIncomingMessages(messages []*protocolpb.Message) error {
	r.RLock()
	defer r.RUnlock()

	// Valid message sequences:
	//  pending -> reject
	//	pending -> accept
	//  accept -> complete

	// make a shallow copy of updates for validation so that it does not alter the state which will be used
	// by a retry of failed workflow task
	updates := make(map[string]*Update, len(r.updates))
	for k, v := range r.updates {
		updates[k] = &Update{
			state:              v.state,
			request:            v.request,
			messageID:          v.messageID,
			protocolInstanceID: v.protocolInstanceID,
			outcome:            nil, // we don't need it for validation
		}
	}

	_, err := processIncomingMessages(updates, messages)
	return err
}

func (r *RegistryImpl) ProcessIncomingMessages(messages []*protocolpb.Message) error {
	r.Lock()
	defer r.Unlock()

	closedUpdates, err := processIncomingMessages(r.updates, messages)
	if err != nil {
		return err
	}

	// notify result to caller
	for _, upd := range closedUpdates {
		upd.notify()
	}
	return nil
}

func processIncomingMessages(updates map[string]*Update, messages []*protocolpb.Message) ([]*Update, error) {
	var closedUpdates []*Update
	for _, message := range messages {
		instanceId := message.GetProtocolInstanceId()
		upd, ok := updates[instanceId]
		if !ok {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("BadUpdateWorkflowExecutionMessage: update %s not found", instanceId))
		}

		if message.GetBody() == nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("BadUpdateWorkflowExecutionMessage: message %s has empty body", instanceId))
		}

		if types.Is(message.GetBody(), (*updatepb.Acceptance)(nil)) {
			if upd.state != statePending {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("BadUpdateWorkflowExecutionMessage: failed to accept update %s, current state: %s", instanceId, upd.state))
			}
			upd.accept()
		} else if types.Is(message.GetBody(), (*updatepb.Response)(nil)) {
			if upd.state != stateAccepted {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("BadUpdateWorkflowExecutionMessage: failed to complete update %s, current state: %s", instanceId, upd.state))
			}
			var response updatepb.Response
			if err := types.UnmarshalAny(message.GetBody(), &response); err != nil {
				return nil, err
			}
			upd.complete(response.GetOutcome())
			closedUpdates = append(closedUpdates, upd)
		} else if types.Is(message.GetBody(), (*updatepb.Rejection)(nil)) {
			if upd.state != statePending {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("BadUpdateWorkflowExecutionMessage: failed to reject update %s, current state: %s", instanceId, upd.state))
			}
			var rejection updatepb.Rejection
			if err := types.UnmarshalAny(message.GetBody(), &rejection); err != nil {
				return nil, err
			}
			upd.reject(rejection.GetFailure())
			closedUpdates = append(closedUpdates, upd)
		} else {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown message type: %s", message.GetBody().GetTypeUrl()))
		}
	}
	return closedUpdates, nil
}

func (r *RegistryImpl) remove(id string) {
	r.Lock()
	defer r.Unlock()
	delete(r.updates, id)
}
