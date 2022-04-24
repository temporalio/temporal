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

package serviceerror

import (
	"fmt"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"

	"go.temporal.io/server/api/errordetails/v1"
)

type (
	// ShardOwnershipLost represents shard ownership lost error.
	ShardOwnershipLost struct {
		Message     string
		OwnerHost   string
		CurrentHost string
		st          *status.Status
	}
)

// NewShardOwnershipLost returns new ShardOwnershipLost error.
func NewShardOwnershipLost(ownerHost string, currentHost string) error {
	return &ShardOwnershipLost{
		Message:     fmt.Sprintf("Shard is owned by:%v but not by %v", ownerHost, currentHost),
		OwnerHost:   ownerHost,
		CurrentHost: currentHost,
	}
}

// Error returns string message.
func (e *ShardOwnershipLost) Error() string {
	return e.Message
}

func (e *ShardOwnershipLost) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Aborted, e.Message)
	st, _ = st.WithDetails(
		&errordetails.ShardOwnershipLostFailure{
			OwnerHost:   e.OwnerHost,
			CurrentHost: e.CurrentHost,
		},
	)
	return st
}

func newShardOwnershipLost(st *status.Status, errDetails *errordetails.ShardOwnershipLostFailure) error {
	return &ShardOwnershipLost{
		Message:     st.Message(),
		OwnerHost:   errDetails.GetOwnerHost(),
		CurrentHost: errDetails.GetCurrentHost(),
		st:          st,
	}
}
