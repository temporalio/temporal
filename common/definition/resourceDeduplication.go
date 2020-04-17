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

package definition

import (
	"fmt"
)

const (
	resourceIDTemplate       = "%v::%v"
	eventReappliedIDTemplate = "%v::%v::%v"
)

type (
	// DeduplicationID uses to generate id for deduplication
	DeduplicationID interface {
		GetID() string
	}
)

// Deduplication id type
const (
	eventReappliedID = iota
)

// Deduplication resource struct
type (
	// EventReappliedID is the deduplication resource for reapply event
	EventReappliedID struct {
		id string
	}
)

// NewEventReappliedID returns EventReappliedID resource
func NewEventReappliedID(
	runID string,
	eventID int64,
	version int64,
) EventReappliedID {

	newID := fmt.Sprintf(
		eventReappliedIDTemplate,
		runID,
		eventID,
		version,
	)
	return EventReappliedID{
		id: newID,
	}
}

// GetID returns id of EventReappliedID
func (e EventReappliedID) GetID() string {
	return e.id
}

// GenerateDeduplicationKey generates deduplication key
func GenerateDeduplicationKey(
	resource DeduplicationID,
) string {

	switch resource.(type) {
	case EventReappliedID:
		return generateKey(eventReappliedID, resource.GetID())
	default:
		panic("unsupported deduplication key")
	}
}

func generateKey(resourceType int32, id string) string {
	return fmt.Sprintf(resourceIDTemplate, resourceType, id)
}
