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
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	resourceDeduplicationSuite struct {
		suite.Suite
	}
)

func TestResourceDeduplicationSuite(t *testing.T) {
	s := new(resourceDeduplicationSuite)
	suite.Run(t, s)
}

func (s *resourceDeduplicationSuite) TestGenerateKey() {
	resourceType := int32(1)
	id := "id"
	key := generateKey(resourceType, id)
	s.Equal(fmt.Sprintf("%v::%v", resourceType, id), key)
}

func (s *resourceDeduplicationSuite) TestGenerateDeduplicationKey() {
	runID := "runID"
	eventID := int64(1)
	version := int64(2)
	resource := NewEventReappliedID(runID, eventID, version)
	key := GenerateDeduplicationKey(resource)
	s.Equal(fmt.Sprintf("%v::%v::%v::%v", eventReappliedID, runID, eventID, version), key)
}

func (s *resourceDeduplicationSuite) TestEventReappliedID() {
	runID := "runID"
	eventID := int64(1)
	version := int64(2)
	resource := NewEventReappliedID(runID, eventID, version)
	s.Equal(fmt.Sprintf("%v::%v::%v", runID, eventID, version), resource.GetID())
}
