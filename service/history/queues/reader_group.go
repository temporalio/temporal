package queues

import (
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common"
)

var (
	errReaderGroupStopped = errors.New("queue reader group already stopped")
)

type (
	ReaderInitializer func(readerID int64, slices []Slice) Reader

	ReaderGroup struct {
		sync.Mutex

		initializer ReaderInitializer

		status    int32
		readerMap map[int64]Reader
	}
)

func NewReaderGroup(
	initializer ReaderInitializer,
) *ReaderGroup {
	return &ReaderGroup{
		initializer: initializer,

		status:    common.DaemonStatusInitialized,
		readerMap: make(map[int64]Reader),
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

func (g *ReaderGroup) ForEach(f func(int64, Reader)) {
	g.Lock()
	defer g.Unlock()

	if g.readerMap == nil {
		return
	}

	for readerID, reader := range g.readerMap {
		f(readerID, reader)
	}
}

func (g *ReaderGroup) Readers() map[int64]Reader {
	g.Lock()
	defer g.Unlock()

	readerMapCopy := make(map[int64]Reader, len(g.readerMap))
	maps.Copy(readerMapCopy, g.readerMap)

	return readerMapCopy
}

func (g *ReaderGroup) GetOrCreateReader(readerID int64) Reader {
	g.Lock()
	defer g.Unlock()

	reader, ok := g.getReaderByIDLocked(readerID)
	if ok {
		return reader
	}

	return g.newReaderLocked(readerID)
}

func (g *ReaderGroup) ReaderByID(readerID int64) (Reader, bool) {
	g.Lock()
	defer g.Unlock()

	return g.getReaderByIDLocked(readerID)
}

func (g *ReaderGroup) getReaderByIDLocked(readerID int64) (Reader, bool) {
	if g.readerMap == nil {
		return nil, false
	}

	reader, ok := g.readerMap[readerID]
	return reader, ok
}

func (g *ReaderGroup) NewReader(readerID int64, slices ...Slice) Reader {
	g.Lock()
	defer g.Unlock()

	return g.newReaderLocked(readerID, slices...)
}

func (g *ReaderGroup) newReaderLocked(readerID int64, slices ...Slice) Reader {
	reader := g.initializer(readerID, slices)

	if _, ok := g.readerMap[readerID]; ok {
		panic(fmt.Sprintf("reader with ID %v already exists", readerID))
	}

	g.readerMap[readerID] = reader

	if g.isStarted() {
		reader.Start()
	}
	return reader
}

func (g *ReaderGroup) RemoveReader(readerID int64) {
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
