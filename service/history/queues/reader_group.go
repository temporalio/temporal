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
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/maps"

	"go.temporal.io/server/common"
)

type (
	ReaderInitializer func(readerID int32, slices []Slice) Reader

	ReaderGroup struct {
		sync.Mutex

		initializer ReaderInitializer

		status    int32
		readerMap map[int32]Reader
	}
)

func NewReaderGroup(
	initializer ReaderInitializer,
) *ReaderGroup {
	return &ReaderGroup{
		initializer: initializer,
		status:      common.DaemonStatusInitialized,
		readerMap:   make(map[int32]Reader),
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

	for _, reader := range g.readerMap {
		reader.Stop()
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

func (g *ReaderGroup) NewReader(readerID int32, slices ...Slice) Reader {
	reader := g.initializer(readerID, slices)

	g.Lock()
	defer g.Unlock()

	if _, ok := g.readerMap[readerID]; ok {
		panic(fmt.Sprintf("reader with ID %v already exists", readerID))
	}

	g.readerMap[readerID] = reader
	if g.isStarted() {
		reader.Start()
	}
	return reader
}

func (g *ReaderGroup) RemoveReader(readerID int32) {
	g.Lock()
	defer g.Unlock()

	reader, ok := g.readerMap[readerID]
	if !ok {
		return
	}

	reader.Stop()
	delete(g.readerMap, readerID)
}

func (g *ReaderGroup) isStarted() bool {
	return atomic.LoadInt32(&g.status) == common.DaemonStatusStarted
}
