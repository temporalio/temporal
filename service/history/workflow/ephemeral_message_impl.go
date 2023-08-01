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

package workflow

type (
	EphemeralMessageChildRunning struct {
		ChildEventID      int64
		ChildEventVersion int64
	}

	EphemeralMessageChildCompletion struct {
		ChildEventID      int64
		ChildEventVersion int64
	}
)

type (
	EphemeralMessages interface {
		Get(key any) (any, bool)
		Set(key any, value any)
		Remove(key any)
	}

	EphemeralMessagesImpl struct {
		ephemeralMessages map[any]any
	}
)

var _ EphemeralMessages = (*EphemeralMessagesImpl)(nil)

func NewEphemeralMessages() *EphemeralMessagesImpl {
	return &EphemeralMessagesImpl{
		ephemeralMessages: make(map[any]any),
	}
}

func (e *EphemeralMessagesImpl) Get(key any) (any, bool) {
	value, ok := e.ephemeralMessages[key]
	return value, ok
}

func (e *EphemeralMessagesImpl) Set(key any, value any) {
	e.ephemeralMessages[key] = value
}

func (e *EphemeralMessagesImpl) Remove(key any) {
	delete(e.ephemeralMessages, key)
}

func NewEphemeralMessageChildRunningKey(
	eventID int64,
	eventVersion int64,
) EphemeralMessageChildRunning {
	return EphemeralMessageChildRunning{
		ChildEventID:      eventID,
		ChildEventVersion: eventVersion,
	}
}

func NewEphemeralMessageChildCompletionKey(
	eventID int64,
	eventVersion int64,
) EphemeralMessageChildCompletion {
	return EphemeralMessageChildCompletion{
		ChildEventID:      eventID,
		ChildEventVersion: eventVersion,
	}
}
