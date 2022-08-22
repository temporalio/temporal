// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

package flusher

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
)

type (
	flusherSuite struct {
		*require.Assertions
		suite.Suite

		ctx context.Context
	}

	fakeTask struct {
		id int64
	}
	fakeWriter struct {
		sync.Mutex
		tasks []*fakeTask
	}
)

func TestFlusher(t *testing.T) {
	fs := new(flusherSuite)
	suite.Run(t, fs)
}

func (s *flusherSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())

	s.Assertions = require.New(s.T())
}

func (s *flusherSuite) TearDownSuite() {

}

func (s *flusherSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *flusherSuite) TearDownTest() {

}

func (s *flusherSuite) TestBuffer_Buffer() {
	bufferCapacity := 2
	numBuffer := 2
	flushTimeout := time.Minute
	writer := &fakeWriter{}
	flushBuffer := NewFlusher[*fakeTask](
		bufferCapacity,
		numBuffer,
		flushTimeout,
		writer,
		log.NewTestLogger(),
	)

	task := newFakeTask()
	fut := flushBuffer.Buffer(task)
	s.False(fut.Ready())

	flushBuffer.Lock()
	defer flushBuffer.Unlock()
	s.Equal(1, len(flushBuffer.flushBuffer))
	s.Equal(task, flushBuffer.flushBuffer[0].Item)
	s.Equal(&flushBuffer.flushBuffer, flushBuffer.flushBufferPointer)
	s.Equal(1, len(flushBuffer.freeBufferChan))
	s.Equal(0, len(flushBuffer.fullBufferChan))

	s.Equal([]*fakeTask{}, writer.Get())
}

func (s *flusherSuite) TestBuffer_Switch() {
	bufferCapacity := 2
	numBuffer := 2
	flushTimeout := time.Minute
	writer := &fakeWriter{}
	flushBuffer := NewFlusher[*fakeTask](
		bufferCapacity,
		numBuffer,
		flushTimeout,
		writer,
		log.NewTestLogger(),
	)

	task0 := newFakeTask()
	task1 := newFakeTask()
	task2 := newFakeTask()
	fut0 := flushBuffer.Buffer(task0)
	fut1 := flushBuffer.Buffer(task1)
	fut2 := flushBuffer.Buffer(task2)
	s.False(fut0.Ready())
	s.False(fut1.Ready())
	s.False(fut2.Ready())

	flushBuffer.Lock()
	defer flushBuffer.Unlock()
	s.Equal(1, len(flushBuffer.flushBuffer))
	s.Equal(task2, flushBuffer.flushBuffer[0].Item)
	s.Equal(&flushBuffer.flushBuffer, flushBuffer.flushBufferPointer)
	s.Equal(0, len(flushBuffer.freeBufferChan))
	s.Equal(1, len(flushBuffer.fullBufferChan))
	buffer := <-flushBuffer.fullBufferChan
	s.Equal(task0, buffer[0].Item)
	s.Equal(task1, buffer[1].Item)
	s.Equal([]*fakeTask{}, writer.Get())
}

func (s *flusherSuite) TestBuffer_Full() {
	bufferCapacity := 1
	numBuffer := 2
	flushTimeout := time.Minute
	writer := &fakeWriter{}
	flushBuffer := NewFlusher[*fakeTask](
		bufferCapacity,
		numBuffer,
		flushTimeout,
		writer,
		log.NewTestLogger(),
	)

	task0 := newFakeTask()
	task1 := newFakeTask()
	task2 := newFakeTask()
	fut0 := flushBuffer.Buffer(task0)
	fut1 := flushBuffer.Buffer(task1)
	fut2 := flushBuffer.Buffer(task2)
	s.False(fut0.Ready())
	s.False(fut1.Ready())
	_, err := fut2.Get(s.ctx)
	s.Equal(ErrFull, err)

	flushBuffer.Lock()
	defer flushBuffer.Unlock()
	s.Equal(0, len(flushBuffer.flushBuffer))
	s.Nil(flushBuffer.flushBufferPointer)
	s.Equal(0, len(flushBuffer.freeBufferChan))
	s.Equal(2, len(flushBuffer.fullBufferChan))
	buffer := <-flushBuffer.fullBufferChan
	s.Equal(task0, buffer[0].Item)
	buffer = <-flushBuffer.fullBufferChan
	s.Equal(task1, buffer[0].Item)
	s.Equal([]*fakeTask{}, writer.Get())
}

func (s *flusherSuite) TestBuffer_Timer() {
	bufferCapacity := 2
	numBuffer := 2
	flushTimeout := time.Millisecond
	writer := &fakeWriter{}
	flushBuffer := NewFlusher[*fakeTask](
		bufferCapacity,
		numBuffer,
		flushTimeout,
		writer,
		log.NewTestLogger(),
	)
	flushBuffer.Start()
	defer flushBuffer.Stop()

	task := newFakeTask()
	fut := flushBuffer.Buffer(task)
	_, err := fut.Get(s.ctx)
	s.NoError(err)

	flushBuffer.Lock()
	s.Equal(0, len(flushBuffer.flushBuffer))
	s.Nil(flushBuffer.flushBufferPointer)
	s.Equal(1, len(flushBuffer.freeBufferChan))
	s.Equal(0, len(flushBuffer.fullBufferChan))
	flushBuffer.Unlock()

	s.Equal([]*fakeTask{task}, writer.Get())
}

func (s *flusherSuite) TestBuffer_Shutdown() {
	bufferCapacity := 2
	numBuffer := 2
	flushTimeout := time.Millisecond
	writer := &fakeWriter{}
	flushBuffer := NewFlusher[*fakeTask](
		bufferCapacity,
		numBuffer,
		flushTimeout,
		writer,
		log.NewTestLogger(),
	)
	flushBuffer.Start()

	task0 := newFakeTask()
	task1 := newFakeTask()
	task2 := newFakeTask()
	fut0 := flushBuffer.Buffer(task0)
	fut1 := flushBuffer.Buffer(task1)
	fut2 := flushBuffer.Buffer(task2)

	flushedTasks := []*fakeTask{}
	flushBuffer.Stop()
	_, err := fut0.Get(s.ctx)
	if err == nil {
		flushedTasks = append(flushedTasks, task0)
	}
	_, err = fut1.Get(s.ctx)
	if err == nil {
		flushedTasks = append(flushedTasks, task1)
	}
	_, err = fut2.Get(s.ctx)
	s.Equal(ErrShutdown, err)

	flushBuffer.Lock()
	s.Nil(flushBuffer.flushBuffer)
	s.Nil(flushBuffer.flushBufferPointer)
	// should not test the len of flushBuffer.freeBufferChan since this buffer is managed async-ly
	s.Equal(0, len(flushBuffer.fullBufferChan))
	flushBuffer.Unlock()

	s.Equal(flushedTasks, writer.Get())
}

func (s *flusherSuite) TestBuffer_Concurrent() {
	bufferCapacity := 128
	numBuffer := 2
	flushTimeout := 4 * time.Millisecond
	writer := &fakeWriter{}
	flushBuffer := NewFlusher[*fakeTask](
		bufferCapacity,
		numBuffer,
		flushTimeout,
		writer,
		log.NewTestLogger(),
	)
	flushBuffer.Start()
	defer flushBuffer.Stop()

	numTaskProducer := bufferCapacity * 2
	numTaskPerProducer := 64

	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}

	startWaitGroup.Add(numTaskProducer)
	endWaitGroup.Add(numTaskProducer)
	for i := 0; i < numTaskProducer; i++ {
		go func() {
			startWaitGroup.Wait()
			defer endWaitGroup.Done()

			for i := 0; i < numTaskPerProducer; i++ {
				task := newFakeTask()
				for {
					fut := flushBuffer.Buffer(task)
					_, err := fut.Get(s.ctx)
					if err != nil {
						time.Sleep(time.Millisecond)
					} else {
						break
					}
				}
			}
		}()
		startWaitGroup.Done()
	}
	endWaitGroup.Wait()

	flushBuffer.Lock()
	s.Equal(0, len(flushBuffer.flushBuffer))
	s.Nil(flushBuffer.flushBufferPointer)
	s.Equal(1, len(flushBuffer.freeBufferChan))
	s.Equal(0, len(flushBuffer.fullBufferChan))
	flushBuffer.Unlock()

	s.Equal(numTaskProducer*numTaskPerProducer, len(writer.Get()))
}

func (w *fakeWriter) Write(
	tasks []*fakeTask,
) error {
	w.Lock()
	defer w.Unlock()
	w.tasks = append(w.tasks, tasks...)
	return nil
}

func (w *fakeWriter) Get() []*fakeTask {
	w.Lock()
	defer w.Unlock()
	tasks := w.tasks
	w.tasks = nil

	if tasks != nil {
		return tasks
	} else {
		return []*fakeTask{}
	}
}

func newFakeTask() *fakeTask {
	return &fakeTask{
		id: rand.Int63(),
	}
}
