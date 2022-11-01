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
	"sync"

	"github.com/gogo/protobuf/types"
	failurepb "go.temporal.io/api/failure/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	updatepb "go.temporal.io/api/update/v1"
)

type (
	Registry interface {
		Add(request *updatepb.Request, afterEventID int64, afterBufferedEventNum int) (*Update, RemoveFunc)

		CreateOutgoingMessages() ([]*protocolpb.Message, error)

		HasPending(filterMessages []*protocolpb.Message) bool
		ProcessIncomingMessages(messages []*protocolpb.Message) error
		UpdateAfterEventID(afterEventID int64)

		Clear()
	}

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

func (r *RegistryImpl) Add(request *updatepb.Request, afterEventID int64, afterBufferedEventNum int) (*Update, RemoveFunc) {
	r.Lock()
	defer r.Unlock()
	protocolInstanceID := request.GetMeta().GetUpdateId()
	upd := newUpdate(request, afterEventID, afterBufferedEventNum, protocolInstanceID)
	r.updates[upd.protocolInstanceID] = upd
	return upd, func() { r.remove(protocolInstanceID) }
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

func (r *RegistryImpl) CreateOutgoingMessages() ([]*protocolpb.Message, error) {
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

	updMessages := make([]*protocolpb.Message, numPendingUpd)
	i := 0
	for _, upd := range r.updates {
		if upd.state == statePending {
			messageBody, err := types.MarshalAny(upd.request)
			if err != nil {
				return nil, err
			}
			updMessages[i] = &protocolpb.Message{
				Id:                 upd.messageID,
				ProtocolInstanceId: upd.protocolInstanceID,
				SequencingId: &protocolpb.Message_EventId{
					EventId: upd.sequenceEventID(),
				},
				Body: messageBody,
			}
			i++
		}
	}
	return updMessages, nil
}

func (r *RegistryImpl) ProcessIncomingMessages(messages []*protocolpb.Message) error {
	r.Lock()
	defer r.Unlock()
	for _, message := range messages {
		if types.Is(message.GetBody(), (*updatepb.Acceptance)(nil)) {
			if pendingUpdate := r.getPendingUpdateNoLock(message.GetProtocolInstanceId()); pendingUpdate != nil {
				pendingUpdate.accept()
			}
		}
		if types.Is(message.GetBody(), (*updatepb.Response)(nil)) {
			if acceptedUpdate := r.getAcceptedUpdateNoLock(message.GetProtocolInstanceId()); acceptedUpdate != nil {
				var response updatepb.Response
				if err := types.UnmarshalAny(message.GetBody(), &response); err != nil {
					return err
				}
				acceptedUpdate.sendComplete(response.GetOutcome())
			}
		}
		if types.Is(message.GetBody(), (*updatepb.Rejection)(nil)) {
			if pendingUpdate := r.getPendingUpdateNoLock(message.GetProtocolInstanceId()); pendingUpdate != nil {
				var rejection updatepb.Rejection
				if err := types.UnmarshalAny(message.GetBody(), &rejection); err != nil {
					return err
				}
				pendingUpdate.sendReject(rejection.GetFailure())
			}
		}
	}
	return nil
}

func (r *RegistryImpl) UpdateAfterEventID(afterEventID int64) {
	r.Lock()
	defer r.Unlock()
	for _, upd := range r.updates {
		// If update doesn't have afterEventID set, it means that it was created after workflow task started,
		// last event Id of last generated event from command was unknown at that moment.
		if upd.state == statePending && upd.afterEventID == 0 {
			upd.afterEventID = afterEventID
		}
	}
}

func (r *RegistryImpl) Clear() {
	r.Lock()
	defer r.Unlock()
	for _, upd := range r.updates {
		upd.sendReject(r.clearFailure())
	}
	r.updates = make(map[string]*Update)
}

func (r *RegistryImpl) getPendingUpdateNoLock(protocolInstanceID string) *Update {
	if upd, ok := r.updates[protocolInstanceID]; ok && upd.state == statePending {
		return upd
	}
	return nil
}

func (r *RegistryImpl) getAcceptedUpdateNoLock(protocolInstanceID string) *Update {
	if upd, ok := r.updates[protocolInstanceID]; ok && upd.state == stateAccepted {
		return upd
	}
	return nil
}

func (r *RegistryImpl) clearFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: "update cleared, please retry",
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{
				NonRetryable: false,
			},
		},
	}
}

func (r *RegistryImpl) remove(id string) {
	r.Lock()
	defer r.Unlock()
	delete(r.updates, id)
}
