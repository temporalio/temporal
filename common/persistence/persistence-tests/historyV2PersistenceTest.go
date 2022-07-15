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

package persistencetests

import (
	"context"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/convert"
	p "go.temporal.io/server/common/persistence"
)

type (
	// HistoryV2PersistenceSuite contains history persistence tests
	HistoryV2PersistenceSuite struct {
		// suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions

		ctx    context.Context
		cancel context.CancelFunc
	}
)

const testForkRunID = "11220000-0000-f000-f000-000000000000"

var historyTestRetryPolicy = createHistoryTestRetryPolicy()

func createHistoryTestRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(time.Millisecond * 50)
	policy.SetMaximumInterval(time.Second * 3)
	policy.SetExpirationInterval(time.Second * 30)

	return policy
}

func isConditionFail(err error) bool {
	switch err.(type) {
	case *p.ConditionFailedError:
		return true
	default:
		return false
	}
}

// SetupSuite implementation
func (s *HistoryV2PersistenceSuite) SetupSuite() {
}

// TearDownSuite implementation
func (s *HistoryV2PersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// SetupTest implementation
func (s *HistoryV2PersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	s.ctx, s.cancel = context.WithTimeout(context.Background(), time.Second*30)
}

// TearDownTest implementation
func (s *HistoryV2PersistenceSuite) TearDownTest() {
	s.cancel()
}

// TestGenUUIDs testing  uuid.New() can generate unique UUID
func (s *HistoryV2PersistenceSuite) TestGenUUIDs() {
	wg := sync.WaitGroup{}
	m := sync.Map{}
	concurrency := 1000
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u := uuid.New()
			m.Store(u, true)
		}()
	}
	wg.Wait()
	cnt := 0
	m.Range(func(k, v interface{}) bool {
		cnt++
		return true
	})
	s.Equal(concurrency, cnt)
}

// TestScanAllTrees test
func (s *HistoryV2PersistenceSuite) TestScanAllTrees() {
	// TODO https://go.temporal.io/server/issues/2458
	if s.ExecutionManager.GetName() != "cassandra" {
		return
	}

	resp, err := s.ExecutionManager.GetAllHistoryTreeBranches(s.ctx, &p.GetAllHistoryTreeBranchesRequest{
		PageSize: 1,
	})
	s.Nil(err)
	s.Equal(0, len(resp.Branches), "some trees were leaked in other tests")

	trees := map[string]bool{}
	totalTrees := 1002
	pgSize := 100

	for i := 0; i < totalTrees; i++ {
		treeID := uuid.NewRandom().String()
		bi, err := s.newHistoryBranch(treeID)
		s.Nil(err)

		events := s.genRandomEvents([]int64{1, 2, 3}, 1)
		err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
		s.Nil(err)
		trees[string(treeID)] = true
	}

	var pgToken []byte
	for {
		resp, err := s.ExecutionManager.GetAllHistoryTreeBranches(s.ctx, &p.GetAllHistoryTreeBranchesRequest{
			PageSize:      pgSize,
			NextPageToken: pgToken,
		})
		s.Nil(err)
		for _, br := range resp.Branches {
			uuidTreeId := br.TreeID
			if trees[uuidTreeId] {
				delete(trees, uuidTreeId)

				s.True(br.ForkTime.UnixNano() > 0)
				s.True(len(br.BranchID) > 0)
				s.Equal("branchInfo", br.Info)
			} else {
				s.Fail("treeID not found", br.TreeID)
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}
		pgToken = resp.NextPageToken
	}

	s.Equal(0, len(trees))
}

// TestReadBranchByPagination test
func (s *HistoryV2PersistenceSuite) TestReadBranchByPagination() {
	treeID := uuid.NewRandom().String()
	bi, err := s.newHistoryBranch(treeID)
	s.Nil(err)

	historyW := &historypb.History{}
	events := s.genRandomEvents([]int64{1, 2, 3}, 0)
	err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
	s.Nil(err)
	historyW.Events = events

	events = s.genRandomEvents([]int64{4}, 0)
	err = s.appendNewNode(bi, events, 2)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{5, 6, 7, 8}, 4)
	err = s.appendNewNode(bi, events, 6)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 1)
	err = s.appendNewNode(bi, events, 3)
	s.Nil(err)
	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 2)
	err = s.appendNewNode(bi, events, 4)
	s.Nil(err)
	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 3)
	err = s.appendNewNode(bi, events, 5)
	s.Nil(err)

	events = s.genRandomEvents([]int64{9}, 4)
	err = s.appendNewNode(bi, events, 7)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	// Start to read from middle, should not return error, but the first batch should be ignored by application layer
	req := &p.ReadHistoryBranchRequest{
		BranchToken:   bi,
		MinEventID:    6,
		MaxEventID:    10,
		PageSize:      4,
		NextPageToken: nil,
		ShardID:       s.ShardInfo.GetShardId(),
	}
	// first page
	resp, err := s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.Nil(err)
	s.Equal(4, len(resp.HistoryEvents))
	s.Equal(int64(6), resp.HistoryEvents[0].GetEventId())

	events = s.genRandomEvents([]int64{10}, 4)
	err = s.appendNewNode(bi, events, 8)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{11}, 4)
	err = s.appendNewNode(bi, events, 9)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{12}, 4)
	err = s.appendNewNode(bi, events, 10)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{13, 14, 15}, 4)
	err = s.appendNewNode(bi, events, 11)
	s.Nil(err)
	// we don't append this batch because we will fork from 13
	// historyW.Events = append(historyW.Events, events...)

	// fork from here
	bi2, err := s.fork(bi, 13)
	s.Nil(err)

	events = s.genRandomEvents([]int64{13}, 4)
	err = s.appendNewNode(bi2, events, 12)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{14}, 4)
	err = s.appendNewNode(bi2, events, 13)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{15, 16, 17}, 4)
	err = s.appendNewNode(bi2, events, 14)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{18, 19, 20}, 4)
	err = s.appendNewNode(bi2, events, 15)
	s.Nil(err)
	historyW.Events = append(historyW.Events, events...)

	// read branch to verify
	historyR := &historypb.History{}

	req = &p.ReadHistoryBranchRequest{
		BranchToken:   bi2,
		MinEventID:    1,
		MaxEventID:    21,
		PageSize:      3,
		NextPageToken: nil,
		ShardID:       s.ShardInfo.GetShardId(),
	}

	// first page
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.Nil(err)

	s.Equal(8, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// this page is all stale batches
	// doe to difference in Cassandra / MySQL pagination
	// the stale event batch may get returned
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.Nil(err)
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken
	if len(resp.HistoryEvents) == 0 {
		// second page
		resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
		s.Nil(err)
		s.Equal(3, len(resp.HistoryEvents))
		historyR.Events = append(historyR.Events, resp.HistoryEvents...)
		req.NextPageToken = resp.NextPageToken
	} else if len(resp.HistoryEvents) == 3 {
		// no op
	} else {
		s.Fail("should either return 0 (Cassandra) or 3 (MySQL) events")
	}

	// 3rd page, since we fork from nodeID=13, we can only see one batch of 12 here
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.Nil(err)
	s.Equal(1, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// 4th page, 13~17
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.Nil(err)
	s.Equal(5, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// last page: one batch of 18-20
	// We have only one page left and the page size is set to one. In this case,
	// persistence may or may not return a nextPageToken.
	// If it does return a token, we need to ensure that if the token returned is used
	// to get history again, no error and history events should be returned.
	req.PageSize = 1
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.Nil(err)
	s.Equal(3, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken
	if len(resp.NextPageToken) != 0 {
		resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
		s.Nil(err)
		s.Equal(0, len(resp.HistoryEvents))
	}

	s.True(reflect.DeepEqual(historyW, historyR))
	s.Equal(0, len(resp.NextPageToken))

	// MinEventID is in the middle of the last batch and this is the first request (NextPageToken
	// is empty), the call should return an error.
	req.MinEventID = 19
	req.NextPageToken = nil
	_, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.IsType(&serviceerror.NotFound{}, err)

	err = s.deleteHistoryBranch(bi2)
	s.Nil(err)
	err = s.deleteHistoryBranch(bi)
	s.Nil(err)
	branches := s.descTree(treeID)
	s.Equal(0, len(branches))
}

// TestConcurrentlyCreateAndAppendBranches test
func (s *HistoryV2PersistenceSuite) TestConcurrentlyCreateAndAppendBranches() {
	treeID := uuid.NewRandom().String()
	wg := sync.WaitGroup{}
	concurrency := 1
	m := sync.Map{}

	// test create new branch along with appending new nodes
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bi, err := s.newHistoryBranch(treeID)
			s.Nil(err)
			historyW := &historypb.History{}
			m.Store(idx, bi)

			events := s.genRandomEvents([]int64{1, 2, 3}, 1)
			err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
			s.Nil(err)
			historyW.Events = events

			events = s.genRandomEvents([]int64{4}, 1)
			err = s.appendNewNode(bi, events, 2)
			s.Nil(err)
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{5, 6, 7, 8}, 1)
			err = s.appendNewNode(bi, events, 3)
			s.Nil(err)
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1)
			err = s.appendNewNode(bi, events, 4000)
			s.Nil(err)
			historyW.Events = append(historyW.Events, events...)

			// read branch to verify
			historyR := &historypb.History{}
			events = s.read(bi, 1, 21)
			s.Equal(20, len(events))
			historyR.Events = events

			s.True(reflect.DeepEqual(historyW, historyR))
		}(i)
	}

	wg.Wait()
	branches := s.descTree(treeID)
	s.Equal(concurrency, len(branches))

	wg = sync.WaitGroup{}
	// test appending nodes(override and new nodes) on each branch concurrently
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			branch := s.getBranchByKey(m, idx)

			// override with smaller txn_id
			events := s.genRandomEvents([]int64{5}, 1)
			err := s.appendNewNode(branch, events, 0)
			s.Nil(err)
			// it shouldn't change anything
			events = s.read(branch, 1, 25)
			s.Equal(20, len(events))

			// override with greatest txn_id
			events = s.genRandomEvents([]int64{5}, 1)
			err = s.appendNewNode(branch, events, 3000)
			s.Nil(err)

			// read to verify override success, at this point history is corrupted, missing 6/7/8, so we should only see 5 events
			events = s.read(branch, 1, 6)
			s.Equal(5, len(events))
			_, err = s.readWithError(branch, 1, 25)
			_, ok := err.(*serviceerror.DataLoss)
			s.Equal(true, ok)

			// override with even larger txn_id and same version
			events = s.genRandomEvents([]int64{5, 6}, 1)
			err = s.appendNewNode(branch, events, 3001)
			s.Nil(err)

			// read to verify override success, at this point history is corrupted, missing 7/8, so we should only see 6 events
			events = s.read(branch, 1, 7)
			s.Equal(6, len(events))
			_, err = s.readWithError(branch, 1, 25)
			_, ok = err.(*serviceerror.DataLoss)
			s.Equal(true, ok)

			// override more with larger txn_id, this would fix the corrupted hole so that we cna get 20 events again
			events = s.genRandomEvents([]int64{7, 8}, 1)
			err = s.appendNewNode(branch, events, 3002)
			s.Nil(err)

			// read to verify override
			events = s.read(branch, 1, 25)
			s.Equal(20, len(events))
			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}, 1)
			err = s.appendNewNode(branch, events, 4001)
			s.Nil(err)
			events = s.read(branch, 1, 25)
			s.Equal(23, len(events))
		}(i)
	}

	wg.Wait()
	// Finally lets clean up all branches
	m.Range(func(k, v interface{}) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		s.Nil(err)
		return true
	})

	branches = s.descTree(treeID)
	s.Equal(0, len(branches))
}

// TestConcurrentlyForkAndAppendBranches test
func (s *HistoryV2PersistenceSuite) TestConcurrentlyForkAndAppendBranches() {
	treeID := uuid.NewRandom().String()
	wg := sync.WaitGroup{}
	concurrency := 10
	masterBr, err := s.newHistoryBranch(treeID)
	s.Nil(err)
	branches := s.descTree(treeID)
	s.Equal(0, len(branches))

	// append first batch to master branch
	eids := []int64{}
	for i := int64(1); i <= int64(concurrency)+1; i++ {
		eids = append(eids, i)
	}
	events := s.genRandomEvents(eids, 1)
	err = s.appendNewBranchAndFirstNode(masterBr, events[0:1], 1, "masterbr")
	s.Nil(err)

	readEvents := s.read(masterBr, 1, int64(concurrency)+2)
	s.Nil(err)
	s.Equal(1, len(readEvents))

	branches = s.descTree(treeID)
	s.Equal(1, len(branches))
	mbrID := branches[0].BranchId

	txn := int64(1)
	getTxnLock := sync.Mutex{}
	reserveTxn := func(count int) int64 {
		getTxnLock.Lock()
		defer getTxnLock.Unlock()

		ret := txn
		txn += int64(count)
		return ret
	}

	err = s.appendOneByOne(masterBr, events[1:], reserveTxn(len(events[1:])))
	s.Nil(err)
	events = s.read(masterBr, 1, int64(concurrency)+2)
	s.Nil(err)
	s.Equal((concurrency)+1, len(events))

	level1ID := sync.Map{}
	level1Br := sync.Map{}
	// test forking from master branch and append nodes
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			forkNodeID := rand.Int63n(int64(concurrency)) + 2
			level1ID.Store(idx, forkNodeID)

			bi, err := s.fork(masterBr, forkNodeID)
			s.Nil(err)
			level1Br.Store(idx, bi)

			// cannot append to ancestors
			events := s.genRandomEvents([]int64{forkNodeID - 1}, 1)
			err = s.appendNewNode(bi, events, reserveTxn(1))
			_, ok := err.(*p.InvalidPersistenceRequestError)
			s.Equal(true, ok)

			// append second batch to first level
			eids := make([]int64, 0)
			for i := forkNodeID; i <= int64(concurrency)*2+1; i++ {
				eids = append(eids, i)
			}
			events = s.genRandomEvents(eids, 1)

			err = s.appendNewNode(bi, events[0:1], reserveTxn(1))
			s.Nil(err)

			err = s.appendOneByOne(bi, events[1:], reserveTxn(len(events[1:])))
			s.Nil(err)

			events = s.read(bi, 1, int64(concurrency)*2+2)
			s.Nil(err)
			s.Equal((concurrency)*2+1, len(events))

			if idx == 0 {
				err = s.deleteHistoryBranch(bi)
				s.Nil(err)
			}

		}(i)
	}

	wg.Wait()
	branches = s.descTreeByToken(masterBr)
	s.Equal(concurrency, len(branches))
	forkOnLevel1 := int32(0)
	level2Br := sync.Map{}
	wg = sync.WaitGroup{}

	// test forking for second level of branch
	for i := 1; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Event we fork from level1 branch, it is possible that the new branch will fork from master branch
			forkNodeID := rand.Int63n(int64(concurrency)*2) + 2
			forkBr := s.getBranchByKey(level1Br, idx)
			lastForkNodeID := s.getIDByKey(level1ID, idx)

			if forkNodeID > lastForkNodeID {
				atomic.AddInt32(&forkOnLevel1, int32(1))
			}

			bi, err := s.fork(forkBr, forkNodeID)
			s.Nil(err)
			level2Br.Store(idx, bi)

			// append second batch to second level
			eids := make([]int64, 0)
			for i := forkNodeID; i <= int64(concurrency)*3+1; i++ {
				eids = append(eids, i)
			}
			events := s.genRandomEvents(eids, 1)
			err = s.appendNewNode(bi, events[0:1], reserveTxn(1))
			s.Nil(err)
			err = s.appendOneByOne(bi, events[1:], reserveTxn(len(events[1:])))
			s.Nil(err)
			events = s.read(bi, 1, int64(concurrency)*3+2)
			s.Nil(err)
			s.Equal((concurrency)*3+1, len(events))

			// try override last event
			events = s.genRandomEvents([]int64{int64(concurrency)*3 + 1}, 1)
			err = s.appendNewNode(bi, events, reserveTxn(1))
			s.Nil(err)
			events = s.read(bi, 1, int64(concurrency)*3+2)
			s.Nil(err)
			s.Equal((concurrency)*3+1, len(events))

			// test fork and newBranch concurrently
			bi, err = s.newHistoryBranch(treeID)
			s.Nil(err)
			level2Br.Store(concurrency+idx, bi)

			events = s.genRandomEvents([]int64{1}, 1)
			err = s.appendNewBranchAndFirstNode(bi, events, reserveTxn(1), "newbr")
			s.Nil(err)

		}(i)
	}

	wg.Wait()
	branches = s.descTree(treeID)
	s.Equal(concurrency*3-2, len(branches))
	actualForkOnLevel1 := int32(0)
	masterCnt := 0
	for _, b := range branches {
		if len(b.Ancestors) == 2 {
			actualForkOnLevel1++
		} else if len(b.Ancestors) == 0 {
			masterCnt++
		} else {
			s.Equal(1, len(b.Ancestors))
			s.Equal(mbrID, b.Ancestors[0].GetBranchId())
		}
	}
	s.Equal(forkOnLevel1, actualForkOnLevel1)
	s.Equal(concurrency, masterCnt)

	// Finally lets clean up all branches
	level1Br.Range(func(k, v interface{}) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		s.Nil(err)

		return true
	})
	level2Br.Range(func(k, v interface{}) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		s.Nil(err)

		return true
	})
	err = s.deleteHistoryBranch(masterBr)
	s.Nil(err)

	branches = s.descTree(treeID)
	s.Equal(0, len(branches))

}

func (s *HistoryV2PersistenceSuite) getBranchByKey(m sync.Map, k int) []byte {
	v, ok := m.Load(k)
	s.Equal(true, ok)
	br := v.([]byte)
	return br
}

func (s *HistoryV2PersistenceSuite) getIDByKey(m sync.Map, k int) int64 {
	v, ok := m.Load(k)
	s.Equal(true, ok)
	id := v.(int64)
	return id
}

func (s *HistoryV2PersistenceSuite) genRandomEvents(eventIDs []int64, version int64) []*historypb.HistoryEvent {
	var events []*historypb.HistoryEvent

	now := time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)
	for _, eid := range eventIDs {
		e := &historypb.HistoryEvent{EventId: eid, Version: version, EventTime: &now}
		events = append(events, e)
	}

	return events
}

// persistence helper
func (s *HistoryV2PersistenceSuite) newHistoryBranch(treeID string) ([]byte, error) {
	return p.NewHistoryBranchToken(treeID)
}

// persistence helper
func (s *HistoryV2PersistenceSuite) deleteHistoryBranch(branch []byte) error {

	op := func() error {
		return s.ExecutionManager.DeleteHistoryBranch(s.ctx, &p.DeleteHistoryBranchRequest{
			BranchToken: branch,
			ShardID:     s.ShardInfo.GetShardId(),
		})
	}

	return backoff.ThrottleRetry(op, historyTestRetryPolicy, isConditionFail)
}

// persistence helper
func (s *HistoryV2PersistenceSuite) descTreeByToken(br []byte) []*persistencespb.HistoryBranch {
	resp, err := s.ExecutionManager.GetHistoryTree(s.ctx, &p.GetHistoryTreeRequest{
		BranchToken: br,
		ShardID:     convert.Int32Ptr(s.ShardInfo.GetShardId()),
	})
	s.Nil(err)
	return resp.Branches
}

func (s *HistoryV2PersistenceSuite) descTree(treeID string) []*persistencespb.HistoryBranch {
	resp, err := s.ExecutionManager.GetHistoryTree(s.ctx, &p.GetHistoryTreeRequest{
		TreeID:  treeID,
		ShardID: convert.Int32Ptr(s.ShardInfo.GetShardId()),
	})
	s.Nil(err)
	return resp.Branches
}

// persistence helper
func (s *HistoryV2PersistenceSuite) read(branch []byte, minID, maxID int64) []*historypb.HistoryEvent {
	res, err := s.readWithError(branch, minID, maxID)
	s.Nil(err)
	return res
}

func (s *HistoryV2PersistenceSuite) readWithError(branch []byte, minID, maxID int64) ([]*historypb.HistoryEvent, error) {

	// use small page size to enforce pagination
	randPageSize := 2
	res := make([]*historypb.HistoryEvent, 0)
	token := []byte{}
	for {
		resp, err := s.ExecutionManager.ReadHistoryBranch(s.ctx, &p.ReadHistoryBranchRequest{
			BranchToken:   branch,
			MinEventID:    minID,
			MaxEventID:    maxID,
			PageSize:      randPageSize,
			NextPageToken: token,
			ShardID:       s.ShardInfo.GetShardId(),
		})
		if err != nil {
			return nil, err
		}
		if len(resp.HistoryEvents) > 0 {
			s.True(resp.Size > 0)
		}
		res = append(res, resp.HistoryEvents...)
		token = resp.NextPageToken
		if len(token) == 0 {
			break
		}
	}

	return res, nil
}

func (s *HistoryV2PersistenceSuite) appendOneByOne(branch []byte, events []*historypb.HistoryEvent, txnID int64) error {
	for index, e := range events {
		err := s.append(branch, []*historypb.HistoryEvent{e}, txnID+int64(index), false, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *HistoryV2PersistenceSuite) appendNewNode(branch []byte, events []*historypb.HistoryEvent, txnID int64) error {
	return s.append(branch, events, txnID, false, "")
}

func (s *HistoryV2PersistenceSuite) appendNewBranchAndFirstNode(branch []byte, events []*historypb.HistoryEvent, txnID int64, branchInfo string) error {
	return s.append(branch, events, txnID, true, branchInfo)
}

// persistence helper
func (s *HistoryV2PersistenceSuite) append(branch []byte, events []*historypb.HistoryEvent, txnID int64, isNewBranch bool, branchInfo string) error {

	var resp *p.AppendHistoryNodesResponse

	op := func() error {
		var err error
		resp, err = s.ExecutionManager.AppendHistoryNodes(s.ctx, &p.AppendHistoryNodesRequest{
			IsNewBranch:   isNewBranch,
			Info:          branchInfo,
			BranchToken:   branch,
			Events:        events,
			TransactionID: txnID,
			ShardID:       s.ShardInfo.GetShardId(),
		})
		return err
	}

	err := backoff.ThrottleRetry(op, historyTestRetryPolicy, isConditionFail)
	if err != nil {
		return err
	}
	s.True(resp.Size > 0)

	return err
}

// persistence helper
func (s *HistoryV2PersistenceSuite) fork(forkBranch []byte, forkNodeID int64) ([]byte, error) {

	bi := []byte{}

	op := func() error {
		var err error
		resp, err := s.ExecutionManager.ForkHistoryBranch(s.ctx, &p.ForkHistoryBranchRequest{
			ForkBranchToken: forkBranch,
			ForkNodeID:      forkNodeID,
			Info:            testForkRunID,
			ShardID:         s.ShardInfo.GetShardId(),
		})
		if resp != nil {
			bi = resp.NewBranchToken
		}
		return err
	}

	err := backoff.ThrottleRetry(op, historyTestRetryPolicy, isConditionFail)
	return bi, err
}
