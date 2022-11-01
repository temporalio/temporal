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
	"context"

	"github.com/pborman/uuid"
	failurepb "go.temporal.io/api/failure/v1"
	updatepb "go.temporal.io/api/update/v1"

	"go.temporal.io/server/common/future"
)

type (
	Update struct {
		state                 state
		request               *updatepb.Request
		afterEventID          int64
		afterBufferedEventNum int
		messageID             string
		protocolInstanceID    string
		out                   *future.FutureImpl[*updatepb.Outcome]
	}

	state int32
)

const (
	statePending state = iota
	stateAccepted
	stateRejected
	stateCompleted
)

func newUpdate(request *updatepb.Request, afterEventID int64, afterBufferedEventNum int, protocolInstanceID string) *Update {
	return &Update{
		state:                 statePending,
		request:               request,
		afterEventID:          afterEventID,
		afterBufferedEventNum: afterBufferedEventNum,
		messageID:             uuid.New(),
		protocolInstanceID:    protocolInstanceID,
		out:                   future.NewFuture[*updatepb.Outcome](),
	}
}

func (u *Update) WaitOutcome(ctx context.Context) (*updatepb.Outcome, error) {
	return u.out.Get(ctx)
}

func (u *Update) sequenceEventID() int64 {
	// TODO (alex-update): fix the formula to take possible reordering into account. See HistoryBuilder.reorderBuffer method.
	return u.afterEventID + int64(u.afterBufferedEventNum)
}

func (u *Update) accept() {
	u.state = stateAccepted
}

func (u *Update) sendComplete(o *updatepb.Outcome) {
	u.state = stateCompleted
	u.out.Set(o, nil)
}

func (u *Update) sendReject(f *failurepb.Failure) {
	u.state = stateRejected
	u.out.Set(&updatepb.Outcome{
		Value: &updatepb.Outcome_Failure{
			Failure: f,
		},
	}, nil)
}
