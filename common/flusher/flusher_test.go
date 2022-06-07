package flusher

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/channel"
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
	shutdownChan := channel.NewShutdownOnce()
	logger := log.NewTestLogger()
	writer := NewItemWriterImpl[int](logger)
	fs.capacity = 200
	bufferQueueSize := 2
	flushDuration := 100

	fs.flusher = NewFlusher[int](bufferQueueSize, fs.capacity, flushDuration, writer, logger, shutdownChan)
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
		futArr[i] = fs.flusher.addItemToBeFlushed(baseVal + i)
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
		futArr[i] = fs.flusher.addItemToBeFlushed(baseVal + i)
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
		futArr[i] = fs.flusher.addItemToBeFlushed(baseVal + i)
	}
	for i := 0; i < fs.capacity+100; i++ {
		_, err := futArr[i].Get(fs.ctx)
		fs.NoError(err)
	}
}
