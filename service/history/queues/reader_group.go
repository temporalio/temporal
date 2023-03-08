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

package queues

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/maps"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type (
	ReaderInitializer func(readerID int32, slices []Slice) Reader

	ReaderGroup struct {
		sync.Mutex

		shardID          int32
		shardOwner       string
		category         tasks.Category
		initializer      ReaderInitializer
		executionManager persistence.ExecutionManager

		status    int32
		readerMap map[int32]Reader
	}
)

func NewReaderGroup(
	shardID int32,
	shardOwner string,
	category tasks.Category,
	initializer ReaderInitializer,
	executionManager persistence.ExecutionManager,
) *ReaderGroup {
	return &ReaderGroup{
		shardID:          shardID,
		shardOwner:       shardOwner,
		category:         category,
		initializer:      initializer,
		executionManager: executionManager,

		status:    common.DaemonStatusInitialized,
		readerMap: make(map[int32]Reader),
	}
}

func (g *ReaderGroup) Start() {
	if !atomic.CompareAndSwapInt32(&g.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	g.Lock()
	defer g.Unlock()

	for _, reader := range g.readerMap {
		reader.Start()
	}
}

func (g *ReaderGroup) Stop() {
	if !atomic.CompareAndSwapInt32(&g.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	g.Lock()
	defer g.Unlock()

	for readerID := range g.readerMap {
		g.removeReaderLocked(readerID)
	}
}

func (g *ReaderGroup) ForEach(f func(int32, Reader)) {
	g.Lock()
	defer g.Unlock()

	for readerID, reader := range g.readerMap {
		f(readerID, reader)
	}
}

func (g *ReaderGroup) Readers() map[int32]Reader {
	g.Lock()
	defer g.Unlock()

	readerMapCopy := make(map[int32]Reader, len(g.readerMap))
	maps.Copy(readerMapCopy, g.readerMap)

	return readerMapCopy
}

func (g *ReaderGroup) ReaderByID(readerID int32) (Reader, bool) {
	g.Lock()
	defer g.Unlock()

	reader, ok := g.readerMap[readerID]
	return reader, ok
}

// TODO: this method should return error
// if reader registration fails or reader group already closed
func (g *ReaderGroup) NewReader(readerID int32, slices ...Slice) Reader {
	reader := g.initializer(readerID, slices)

	g.Lock()
	defer g.Unlock()

	if _, ok := g.readerMap[readerID]; ok {
		panic(fmt.Sprintf("reader with ID %v already exists", readerID))
	}

	// TODO: return error if reader group already stopped

	g.readerMap[readerID] = reader
	err := g.executionManager.RegisterHistoryTaskReader(context.Background(), &persistence.RegisterHistoryTaskReaderRequest{
		ShardID:      g.shardID,
		ShardOwner:   g.shardOwner,
		TaskCategory: g.category,
		ReaderID:     readerID,
	})
	if err != nil {
		// TODO: bubble up the error when registring task reader
		panic(fmt.Sprintf("Unable to register history task reader: %v", err))
	}
	if g.isStarted() {
		reader.Start()
	}
	return reader
}

func (g *ReaderGroup) RemoveReader(readerID int32) {
	g.Lock()
	defer g.Unlock()

	g.removeReaderLocked(readerID)
}

func (g *ReaderGroup) removeReaderLocked(readerID int32) {
	reader, ok := g.readerMap[readerID]
	if !ok {
		return
	}

	// TODO: reader.Stop() does not guarantee reader won't issue new read requests
	// But it's very unlikely as it waits for 1min.
	// If UnregisterHistoryTaskReader requires no more read requests after the call
	// we need to wait for the separate reader goroutine to complete.
	reader.Stop()
	g.executionManager.UnregisterHistoryTaskReader(context.Background(), &persistence.UnregisterHistoryTaskReaderRequest{
		ShardID:      g.shardID,
		ShardOwner:   g.shardOwner,
		TaskCategory: g.category,
		ReaderID:     readerID,
	})
	delete(g.readerMap, readerID)
}

func (g *ReaderGroup) isStarted() bool {
	return atomic.LoadInt32(&g.status) == common.DaemonStatusStarted
}
