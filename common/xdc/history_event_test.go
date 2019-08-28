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

package xdc

import (
	"fmt"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"runtime/debug"
	"testing"
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
	s.generator = InitializeHistoryEventGenerator()
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

	totalBranchNumber := 2
	currentBranch := totalBranchNumber
	root := &NDCTestBranch{
		Batches: make([]NDCTestBatch, 0),
	}
	curr := root
	//eventRanches := make([][]Vertex, 0, totalBranchNumber)
	for currentBranch > 0 {
		for s.generator.HasNextVertex() {
			events := s.generator.GetNextVertices()
			newBatch := NDCTestBatch{
				Events: events,
			}
			curr.Batches = append(curr.Batches, newBatch)
			for _, e := range events {
				fmt.Println(e.GetName())
			}
		}
		currentBranch--
		if currentBranch > 0 {
			resetIdx := s.generator.RandomResetToResetPoint()
			curr = root.split(resetIdx)
		}
	}
	s.NotEmpty(s.generator.ListGeneratedVertices())
	queue := []*NDCTestBranch{root}
	for len(queue) > 0 {
		b := queue[0]
		queue = queue[1:]
		for _, batch := range b.Batches {
			for _, event := range batch.Events {
				fmt.Println(event.GetName())
			}
		}
		queue = append(queue, b.Next...)
	}

	// Generator one branch of history events
	batches := []NDCTestBatch{}
	batches = append(batches, root.Batches...)
	batches = append(batches, root.Next[0].Batches...)
	identity := "test-event-generator"
	wid := uuid.New()
	rid := uuid.New()
	wt := "event-generator-workflow-type"
	tl := "event-generator-taskList"
	domain := "event-generator"
	domainID := uuid.New()
	attributeGenerator := NewHistoryAttributesGenerator(wid, rid, tl, wt, domain, domainID, identity)
	history := attributeGenerator.GenerateHistoryEvents(batches, 1, 100)
	s.NotEmpty(history)
}
