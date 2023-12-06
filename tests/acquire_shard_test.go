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

package tests

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// TestAcquireShard_OwnershipLostErrorSuite tests what happens when acquire shard returns an ownership lost error.
func TestAcquireShard_OwnershipLostErrorSuite(t *testing.T) {
	s := new(OwnershipLostErrorSuite)
	suite.Run(t, s)
}

// TestAcquireShard_DeadlineExceededErrorSuite tests what happens when acquire shard returns a deadline exceeded error
func TestAcquireShard_DeadlineExceededErrorSuite(t *testing.T) {
	s := new(DeadlineExceededErrorSuite)
	suite.Run(t, s)
}

// TestAcquireShard_EventualSuccess verifies that we eventually succeed in acquiring the shard when we get a deadline
// exceeded error followed by a successful acquire shard call.
// To make this test deterministic, we set the seed to in the config file to a fixed value.
func TestAcquireShard_EventualSuccess(t *testing.T) {
	s := new(EventualSuccessSuite)
	suite.Run(t, s)
}
