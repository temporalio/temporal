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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/maps"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

var (
	errReaderGroupStopped = errors.New("queue reader group already stopped")
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

	// Stop() method is protected by g.status, so g.readerMap will never be nil here
	for readerID := range g.readerMap {
		g.removeReaderLocked(readerID)
	}

	// This guarantee no new reader can be created/registered after Stop() returns
	// also all registered readers will be unregistered.
	g.readerMap = nil
}

func (g *ReaderGroup) ForEach(f func(int32, Reader)) {
	g.Lock()
	defer g.Unlock()

	if g.readerMap == nil {
		return
	}

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

func (g *ReaderGroup) GetOrCreateReader(readerID int32) (Reader, error) {
	g.Lock()
	defer g.Unlock()

	reader, ok := g.getReaderByIDLocked(readerID)
	if ok {
		return reader, nil
	}

	return g.newReaderLocked(readerID)
}

func (g *ReaderGroup) ReaderByID(readerID int32) (Reader, bool) {
	g.Lock()
	defer g.Unlock()

	return g.getReaderByIDLocked(readerID)
}

func (g *ReaderGroup) getReaderByIDLocked(readerID int32) (Reader, bool) {
	if g.readerMap == nil {
		return nil, false
	}

	reader, ok := g.readerMap[readerID]
	return reader, ok
}

func (g *ReaderGroup) NewReader(readerID int32, slices ...Slice) (Reader, error) {
	g.Lock()
	defer g.Unlock()

	return g.newReaderLocked(readerID, slices...)
}

func (g *ReaderGroup) newReaderLocked(readerID int32, slices ...Slice) (Reader, error) {
	reader := g.initializer(readerID, slices)

	if g.readerMap == nil {
		return nil, errReaderGroupStopped
	}

	if _, ok := g.readerMap[readerID]; ok {
		panic(fmt.Sprintf("reader with ID %v already exists", readerID))
	}

	g.readerMap[readerID] = reader
	err := g.executionManager.RegisterHistoryTaskReader(context.Background(), &persistence.RegisterHistoryTaskReaderRequest{
		ShardID:      g.shardID,
		ShardOwner:   g.shardOwner,
		TaskCategory: g.category,
		ReaderID:     readerID,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to register history task reader: %w", err)
	}
	if g.isStarted() {
		reader.Start()
	}
	return reader, nil
}

func (g *ReaderGroup) RemoveReader(readerID int32) {
	g.Lock()
	defer g.Unlock()

	g.removeReaderLocked(readerID)
}

func (g *ReaderGroup) removeReaderLocked(readerID int32) {
	if g.readerMap == nil {
		return
	}

	reader, ok := g.readerMap[readerID]
	if !ok {
		return
	}

	// NOTE: reader.Stop() does not guarantee reader won't issue new read requests
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
