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
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"sync"
	"testing"
)

type (
	flusherSuite struct {
		*require.Assertions
		suite.Suite
		controller *gomock.Controller
		ctx        context.Context

		capacity int
		sync.Mutex
		flusher *Flusher[int]
	}
)

func TestFlusher(t *testing.T) {
	fs := new(flusherSuite)
	suite.Run(t, fs)
}

func (fs *flusherSuite) SetupSuite() {
	fs.Assertions = require.New(fs.T())
}

func (fs *flusherSuite) TearDownSuite() {
	fs.controller.Finish()
}

func (fs *flusherSuite) SetupTest() {
	fs.controller = gomock.NewController(fs.T())
	fs.ctx = context.Background()
	logger := log.NewTestLogger()
	writer := NewItemWriter[int](logger)
	fs.capacity = 200
	bufferCount := 2
	flushDuration := 100

	fs.flusher = NewFlusher[int](bufferCount, fs.capacity, flushDuration, writer, logger)
	fs.flusher.Start()
}

func (fs *flusherSuite) TearDownTest() {
	fs.controller.Finish()
}

//=============================================================================

func (fs *flusherSuite) TestFlushTimer() {
	var futArr [200]*future.FutureImpl[struct{}]
	baseVal := 25
	for i := 0; i < fs.capacity-160; i++ {
		futArr[i] = fs.flusher.AddItemToBeFlushed(baseVal + i)
	}
	for i := 0; i < fs.capacity-160; i++ {
		_, err := futArr[i].Get(fs.ctx)
		fs.NoError(err)
	}
}

func (fs *flusherSuite) TestBufferFullFlush() {
	var futArr [200]*future.FutureImpl[struct{}]
	baseVal := 25
	for i := 0; i < fs.capacity; i++ {
		futArr[i] = fs.flusher.AddItemToBeFlushed(baseVal + i)
	}
	for i := 0; i < fs.capacity; i++ {
		_, err := futArr[i].Get(fs.ctx)
		fs.NoError(err)
	}
}

func (fs *flusherSuite) TestBufferPastCapacityFlush() {
	var futArr [300]*future.FutureImpl[struct{}]
	baseVal := 25
	for i := 0; i < fs.capacity+100; i++ {
		futArr[i] = fs.flusher.AddItemToBeFlushed(baseVal + i)
	}
	for i := 0; i < fs.capacity+100; i++ {
		_, err := futArr[i].Get(fs.ctx)
		fs.NoError(err)
	}
}
