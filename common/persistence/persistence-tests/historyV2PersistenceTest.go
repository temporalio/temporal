// Copyright (c) 2017 Uber Technologies, Inc.
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
	"os"
	"testing"

	"time"

	"sync/atomic"

	"sync"

	"math/rand"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	p "github.com/uber/cadence/common/persistence"
)

type (
	// HistoryV2PersistenceSuite contains history persistence tests
	HistoryV2PersistenceSuite struct {
		suite.Suite
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

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
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

// SetupTest implementation
func (s *HistoryV2PersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

// TearDownSuite implementation
func (s *HistoryV2PersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
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

//TestConcurrentlyCreateAndAppendBranches test
func (s *HistoryV2PersistenceSuite) TestConcurrentlyCreateAndAppendBranches() {
	treeID := uuid.New()

	wg := sync.WaitGroup{}
	concurrency := 20
	m := sync.Map{}

	// test create new branch along with appending new nodes
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bi, err := s.newHistoryBranch(treeID)
			s.Nil(err)
			historyW := &workflow.History{}
			m.Store(idx, bi)

			events := s.genRandomEvents([]int64{1, 2, 3}, 1)
			err = s.append(bi, events, 1, true)
			s.Nil(err)
			historyW.Events = events

			events = s.genRandomEvents([]int64{4}, 1)
			err = s.append(bi, events, 1, false)
			s.Nil(err)
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{5, 6, 7, 8}, 1)
			err = s.append(bi, events, 1, false)
			s.Nil(err)
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1)
			err = s.append(bi, events, 1, false)
			s.Nil(err)
			historyW.Events = append(historyW.Events, events...)

			//read branch to verify
			historyR := &workflow.History{}
			events = s.read(bi, 1, 21)
			s.Equal(20, len(events))
			historyR.Events = events

			s.True(historyW.Equals(historyR))
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
			err := s.append(branch, events, 0, false)
			s.Nil(err)
			// it shouldn't change anything
			events = s.read(branch, 1, 25)
			s.Equal(20, len(events))

			// override with same txn_id but greater version
			events = s.genRandomEvents([]int64{5}, 2)
			err = s.append(branch, events, 1, false)
			s.Nil(err)

			// read to verify override success
			events = s.read(branch, 1, 25)
			s.Equal(5, len(events))

			// override with larger txn_id and same version
			events = s.genRandomEvents([]int64{5, 6}, 1)
			err = s.append(branch, events, 2, false)
			s.Nil(err)

			// read to verify override success, at this point history is corrupted, missing 7/8, so we should only see 6 events
			_, err = s.readWithError(branch, 1, 25)
			_, ok := err.(*workflow.InternalServiceError)
			s.Equal(true, ok)

			events = s.read(branch, 1, 7)
			s.Equal(6, len(events))

			// override more with larger txn_id, this would fix the corrupted hole so that we cna get 20 events again
			events = s.genRandomEvents([]int64{7, 8}, 1)
			err = s.append(branch, events, 2, false)
			s.Nil(err)

			// read to verify override
			events = s.read(branch, 1, 25)
			s.Equal(20, len(events))

			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}, 1)
			err = s.append(branch, events, 2, false)
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
	treeID := uuid.New()

	wg := sync.WaitGroup{}
	concurrency := 20
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
	err = s.appendOneByOne(masterBr, events, 1)
	s.Nil(err)
	events = s.read(masterBr, 1, int64(concurrency)+2)
	s.Nil(err)
	s.Equal((concurrency)+1, len(events))

	branches = s.descTree(treeID)
	s.Equal(1, len(branches))
	mbrID := *branches[0].BranchID

	// cannot fork on un-existing node
	_, err = s.fork(masterBr, int64(concurrency)+2)
	_, ok := err.(*p.InvalidPersistenceRequestError)
	s.Equal(true, ok)

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

			//cannot append to ancestors
			events := s.genRandomEvents([]int64{forkNodeID - 1}, 1)
			err = s.append(bi, events, 1, true)
			_, ok := err.(*p.InvalidPersistenceRequestError)
			s.Equal(true, ok)

			// append second batch to first level
			eids := make([]int64, 0)
			for i := forkNodeID; i <= int64(concurrency)*2+1; i++ {
				eids = append(eids, i)
			}
			events = s.genRandomEvents(eids, 1)

			err = s.appendOneByOne(bi, events, 1)
			s.Nil(err)

			events = s.read(bi, 1, int64(concurrency)*2+2)
			s.Nil(err)
			s.Equal((concurrency)*2+1, len(events))
		}(i)
	}

	wg.Wait()
	branches = s.descTree(treeID)
	s.Equal(concurrency+1, len(branches))
	forkOnLevel1 := int32(0)
	level2Br := sync.Map{}
	wg = sync.WaitGroup{}

	// test forking for second level of branch
	for i := 0; i < concurrency; i++ {
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
			err = s.appendOneByOne(bi, events, 1)
			s.Nil(err)
			events = s.read(bi, 1, int64(concurrency)*3+2)
			s.Nil(err)
			s.Equal((concurrency)*3+1, len(events))

			// try override last event
			events = s.genRandomEvents([]int64{int64(concurrency)*3 + 1}, 1)
			err = s.append(bi, events, 2, false)
			s.Nil(err)
			events = s.read(bi, 1, int64(concurrency)*3+2)
			s.Nil(err)
			s.Equal((concurrency)*3+1, len(events))

			//test fork and newBranch concurrently
			bi, err = s.newHistoryBranch(treeID)
			s.Nil(err)
			level2Br.Store(concurrency+idx, bi)

			events = s.genRandomEvents([]int64{1}, 1)
			err = s.append(bi, events, 0, true)
			s.Nil(err)
		}(i)
	}

	wg.Wait()
	branches = s.descTree(treeID)
	s.Equal(int(concurrency*3+1), len(branches))
	actualForkOnLevel1 := int32(0)
	masterCnt := 0
	for _, b := range branches {
		if len(b.Ancestors) == 2 {
			actualForkOnLevel1++
		} else if len(b.Ancestors) == 0 {
			masterCnt++
		} else {
			s.Equal(1, len(b.Ancestors))
			s.Equal(mbrID, *b.Ancestors[0].BranchID)
		}
	}
	s.Equal(forkOnLevel1, actualForkOnLevel1)
	s.Equal(1+concurrency, masterCnt)

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

func (s *HistoryV2PersistenceSuite) genRandomEvents(eventIDs []int64, version int64) []*workflow.HistoryEvent {
	var events []*workflow.HistoryEvent

	timestamp := time.Now().UnixNano()
	for _, eid := range eventIDs {
		e := &workflow.HistoryEvent{EventId: common.Int64Ptr(eid), Version: common.Int64Ptr(version), Timestamp: int64Ptr(timestamp)}
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
		var err error
		err = s.HistoryV2Mgr.DeleteHistoryBranch(&p.DeleteHistoryBranchRequest{
			BranchToken: branch,
		})
		return err
	}

	return backoff.Retry(op, historyTestRetryPolicy, isConditionFail)
}

// persistence helper
func (s *HistoryV2PersistenceSuite) descTree(treeID string) []*workflow.HistoryBranch {
	resp, err := s.HistoryV2Mgr.GetHistoryTree(&p.GetHistoryTreeRequest{
		TreeID: treeID,
	})
	s.Nil(err)
	return resp.Branches
}

// persistence helper
func (s *HistoryV2PersistenceSuite) read(branch []byte, minID, maxID int64) []*workflow.HistoryEvent {
	res, err := s.readWithError(branch, minID, maxID)
	s.Nil(err)
	return res
}

func (s *HistoryV2PersistenceSuite) readWithError(branch []byte, minID, maxID int64) ([]*workflow.HistoryEvent, error) {

	// 0 or 1, randomly use small page size or large page size
	ri := rand.Intn(2)
	randPageSize := 2
	if ri == 0 {
		randPageSize = 100
	}
	res := make([]*workflow.HistoryEvent, 0)
	token := []byte{}
	for {
		resp, err := s.HistoryV2Mgr.ReadHistoryBranch(&p.ReadHistoryBranchRequest{
			BranchToken:   branch,
			MinEventID:    minID,
			MaxEventID:    maxID,
			PageSize:      randPageSize,
			NextPageToken: token,
		})
		if err != nil {
			return nil, err
		}
		if len(resp.History) > 0 {
			s.True(resp.Size > 0)
		}
		res = append(res, resp.History...)
		token = resp.NextPageToken
		if len(token) == 0 {
			break
		}
	}

	return res, nil
}

func (s *HistoryV2PersistenceSuite) appendOneByOne(branch []byte, events []*workflow.HistoryEvent, txnID int64) error {
	err := s.append(branch, []*workflow.HistoryEvent{events[0]}, txnID, true)
	if err != nil {
		return err
	}
	for _, e := range events {
		err := s.append(branch, []*workflow.HistoryEvent{e}, txnID, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// persistence helper
func (s *HistoryV2PersistenceSuite) append(branch []byte, events []*workflow.HistoryEvent, txnID int64, isNewBranch bool) error {

	var resp *p.AppendHistoryNodesResponse

	op := func() error {
		var err error
		resp, err = s.HistoryV2Mgr.AppendHistoryNodes(&p.AppendHistoryNodesRequest{
			IsNewBranch:   isNewBranch,
			BranchToken:   branch,
			Events:        events,
			TransactionID: txnID,
			Encoding:      pickRandomEncoding(),
		})
		return err
	}

	err := backoff.Retry(op, historyTestRetryPolicy, isConditionFail)
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
		resp, err := s.HistoryV2Mgr.ForkHistoryBranch(&p.ForkHistoryBranchRequest{
			ForkBranchToken: forkBranch,
			ForkNodeID:      forkNodeID,
		})
		if resp != nil {
			bi = resp.NewBranchToken
		}
		return err
	}

	err := backoff.Retry(op, historyTestRetryPolicy, isConditionFail)
	return bi, err
}
