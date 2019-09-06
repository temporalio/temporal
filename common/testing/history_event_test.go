// Copyright (c) 2019 Uber Technologies, Inc.
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

package testing

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
)

type (
	historyEventTestSuit struct {
		suite.Suite
		generator Generator
	}
)

func TestHistoryEventTestSuite(t *testing.T) {
	suite.Run(t, new(historyEventTestSuit))
}

func (s *historyEventTestSuit) SetupSuite() {
	s.generator = InitializeHistoryEventGenerator("domain", 1)
}

func (s *historyEventTestSuit) SetupTest() {
	s.generator.Reset()
}

// This is a sample about how to use the generator
func (s *historyEventTestSuit) Test_HistoryEvent_Generator() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
		}
	}()
	maxEventID := int64(0)
	for s.generator.HasNextVertex() {
		events := s.generator.GetNextVertices()
		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*shared.HistoryEvent)
			if maxEventID > event.GetEventId() {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.GetEventId()
			fmt.Println(e.GetName())
			fmt.Println(event.GetEventId())
		}
	}
	s.NotEmpty(s.generator.ListGeneratedVertices())
	fmt.Println("==========================")
	newGenerator := s.generator.ResetToResetPoint(0)
	maxEventID = int64(0)

	for newGenerator.HasNextVertex() {
		events := newGenerator.GetNextVertices()
		fmt.Println("########################")
		for _, e := range events {
			event := e.GetData().(*shared.HistoryEvent)
			if maxEventID > event.GetEventId() {
				s.Fail("event id sequence is incorrect")
			}
			maxEventID = event.GetEventId()
			fmt.Println(e.GetName())
			fmt.Println(event.GetEventId())
		}
	}
	s.NotEmpty(newGenerator.ListGeneratedVertices())
}
