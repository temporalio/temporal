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

type (
	state    uint32
	stateSet uint32
)

const (
	stateCreated state = 1 << iota
	stateProvisionallyAdmitted
	stateAdmitted
	stateSent
	stateProvisionallyAccepted
	stateAccepted
	stateProvisionallyCompleted
	stateCompleted
	stateAborted
)

func (s state) String() string {
	switch s {
	case stateCreated:
		return "Created"
	case stateProvisionallyAdmitted:
		return "ProvisionallyAdmitted"
	case stateAdmitted:
		return "Admitted"
	case stateSent:
		return "Sent"
	case stateProvisionallyAccepted:
		return "ProvisionallyAccepted"
	case stateAccepted:
		return "Accepted"
	case stateProvisionallyCompleted:
		return "ProvisionallyCompleted"
	case stateCompleted:
		return "Completed"
	case stateAborted:
		return "Aborted"
	}
	return "unrecognized state"
}

func (s state) Matches(mask stateSet) bool {
	return uint32(s)&uint32(mask) == uint32(s)
}
