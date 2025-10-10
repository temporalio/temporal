package persistencetests

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/debug"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// HistoryV2PersistenceSuite contains history persistence tests
	HistoryV2PersistenceSuite struct {
		// suite.Suite
		*TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that require.NotNil(s.T(), nil) will stop the test,
		// not merely log an error
		protorequire.ProtoAssertions

		ctx    context.Context
		cancel context.CancelFunc
	}
)

const testForkRunID = "11220000-0000-f000-f000-000000000000"

var (
	historyTestRetryPolicy = backoff.NewExponentialRetryPolicy(time.Millisecond * 50).
		WithMaximumInterval(time.Second * 3).
		WithExpirationInterval(time.Second * 30)
)

func isConditionFail(err error) bool {
	switch err.(type) {
	case *p.ConditionFailedError:
		return true
	default:
		return false
	}
}

// SetupSuite implementation


// TearDownSuite implementation
func (s *HistoryV2PersistenceSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

// SetupTest implementation
func (s *HistoryV2PersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil

	s.ProtoAssertions = protorequire.New(s.T())

	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
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
	require.Equal(s.T(), concurrency, cnt)
}

// TestScanAllTrees test
func (s *HistoryV2PersistenceSuite) TestScanAllTrees() {
	resp, err := s.ExecutionManager.GetAllHistoryTreeBranches(s.ctx, &p.GetAllHistoryTreeBranchesRequest{
		PageSize: 1,
	})
	require.Nil(s.T(), err)
	require.Equal(s.T(), 0, len(resp.Branches), "some trees were leaked in other tests")

	trees := map[string]bool{}
	totalTrees := 1002
	pgSize := 100

	for i := 0; i < totalTrees; i++ {
		treeID := uuid.NewRandom().String()
		bi, err := s.newHistoryBranch(treeID)
		require.Nil(s.T(), err)

		events := s.genRandomEvents([]int64{1, 2, 3}, 1)
		err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
		require.Nil(s.T(), err)
		trees[string(treeID)] = true
	}

	var pgToken []byte
	for {
		resp, err := s.ExecutionManager.GetAllHistoryTreeBranches(s.ctx, &p.GetAllHistoryTreeBranchesRequest{
			PageSize:      pgSize,
			NextPageToken: pgToken,
		})
		require.Nil(s.T(), err)
		for _, br := range resp.Branches {
			uuidTreeId := br.BranchInfo.TreeId
			if trees[uuidTreeId] {
				delete(trees, uuidTreeId)

				require.True(s.T(), br.ForkTime.AsTime().UnixNano() > 0)
				require.True(s.T(), len(br.BranchInfo.BranchId) > 0)
				require.Equal(s.T(), "branchInfo", br.Info)
			} else {
				require.Fail(s.T(), "treeID not found", br.BranchInfo.TreeId)
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}
		pgToken = resp.NextPageToken
	}

	require.Equal(s.T(), 0, len(trees))
}

// TestReadBranchByPagination test
func (s *HistoryV2PersistenceSuite) TestReadBranchByPagination() {
	treeID := uuid.NewRandom().String()
	bi, err := s.newHistoryBranch(treeID)
	require.Nil(s.T(), err)

	historyW := &historypb.History{}
	events := s.genRandomEvents([]int64{1, 2, 3}, 0)
	err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
	require.Nil(s.T(), err)
	historyW.Events = events

	events = s.genRandomEvents([]int64{4}, 0)
	err = s.appendNewNode(bi, events, 2)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{5, 6, 7, 8}, 4)
	err = s.appendNewNode(bi, events, 6)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 1)
	err = s.appendNewNode(bi, events, 3)
	require.Nil(s.T(), err)
	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 2)
	err = s.appendNewNode(bi, events, 4)
	require.Nil(s.T(), err)
	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 3)
	err = s.appendNewNode(bi, events, 5)
	require.Nil(s.T(), err)

	events = s.genRandomEvents([]int64{9}, 4)
	err = s.appendNewNode(bi, events, 7)
	require.Nil(s.T(), err)
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
	require.Nil(s.T(), err)
	require.Equal(s.T(), 4, len(resp.HistoryEvents))
	require.Equal(s.T(), int64(6), resp.HistoryEvents[0].GetEventId())

	events = s.genRandomEvents([]int64{10}, 4)
	err = s.appendNewNode(bi, events, 8)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{11}, 4)
	err = s.appendNewNode(bi, events, 9)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{12}, 4)
	err = s.appendNewNode(bi, events, 10)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{13, 14, 15}, 4)
	err = s.appendNewNode(bi, events, 11)
	require.Nil(s.T(), err)
	// we don't append this batch because we will fork from 13
	// historyW.Events = append(historyW.Events, events...)

	// fork from here
	bi2, err := s.fork(bi, 13)
	require.Nil(s.T(), err)

	events = s.genRandomEvents([]int64{13}, 4)
	err = s.appendNewNode(bi2, events, 12)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{14}, 4)
	err = s.appendNewNode(bi2, events, 13)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{15, 16, 17}, 4)
	err = s.appendNewNode(bi2, events, 14)
	require.Nil(s.T(), err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{18, 19, 20}, 4)
	err = s.appendNewNode(bi2, events, 15)
	require.Nil(s.T(), err)
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
	require.Nil(s.T(), err)

	require.Equal(s.T(), 8, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// this page is all stale batches
	// doe to difference in Cassandra / MySQL pagination
	// the stale event batch may get returned
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	require.Nil(s.T(), err)
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken
	if len(resp.HistoryEvents) == 0 {
		// second page
		resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
		require.Nil(s.T(), err)
		require.Equal(s.T(), 3, len(resp.HistoryEvents))
		historyR.Events = append(historyR.Events, resp.HistoryEvents...)
		req.NextPageToken = resp.NextPageToken
	} else if len(resp.HistoryEvents) == 3 {
		// no op
	} else {
		require.Fail(s.T(), "should either return 0 (Cassandra) or 3 (MySQL) events")
	}

	// 3rd page, since we fork from nodeID=13, we can only see one batch of 12 here
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	require.Nil(s.T(), err)
	require.Equal(s.T(), 1, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// 4th page, 13~17
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	require.Nil(s.T(), err)
	require.Equal(s.T(), 5, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// last page: one batch of 18-20
	// We have only one page left and the page size is set to one. In this case,
	// persistence may or may not return a nextPageToken.
	// If it does return a token, we need to ensure that if the token returned is used
	// to get history again, no error and history events should be returned.
	req.PageSize = 1
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	require.Nil(s.T(), err)
	require.Equal(s.T(), 3, len(resp.HistoryEvents))
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken
	if len(resp.NextPageToken) != 0 {
		resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
		require.Nil(s.T(), err)
		require.Equal(s.T(), 0, len(resp.HistoryEvents))
	}

	s.ProtoEqual(historyW, historyR)
	require.Equal(s.T(), 0, len(resp.NextPageToken))

	// MinEventID is in the middle of the last batch and this is the first request (NextPageToken
	// is empty), the call should return an error.
	req.MinEventID = 19
	req.NextPageToken = nil
	_, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	require.IsType(s.T(), &serviceerror.NotFound{}, err)

	err = s.deleteHistoryBranch(bi2)
	require.Nil(s.T(), err)
	err = s.deleteHistoryBranch(bi)
	require.Nil(s.T(), err)
	branches := s.descTree(treeID)
	require.Equal(s.T(), 0, len(branches))
}

// TestConcurrentlyCreateAndAppendBranches test
func (s *HistoryV2PersistenceSuite) TestConcurrentlyCreateAndAppendBranches() {
	treeID := uuid.NewRandom().String()
	wg := sync.WaitGroup{}
	concurrency := 1
	m := &sync.Map{}

	// test create new branch along with appending new nodes
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bi, err := s.newHistoryBranch(treeID)
			require.Nil(s.T(), err)
			historyW := &historypb.History{}
			m.Store(idx, bi)

			events := s.genRandomEvents([]int64{1, 2, 3}, 1)
			err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
			require.Nil(s.T(), err)
			historyW.Events = events

			events = s.genRandomEvents([]int64{4}, 1)
			err = s.appendNewNode(bi, events, 2)
			require.Nil(s.T(), err)
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{5, 6, 7, 8}, 1)
			err = s.appendNewNode(bi, events, 3)
			require.Nil(s.T(), err)
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1)
			err = s.appendNewNode(bi, events, 4000)
			require.Nil(s.T(), err)
			historyW.Events = append(historyW.Events, events...)

			// read branch to verify
			historyR := &historypb.History{}
			events = s.read(bi, 1, 21)
			require.Equal(s.T(), 20, len(events))
			historyR.Events = events

			s.ProtoEqual(historyW, historyR)
		}(i)
	}

	wg.Wait()
	branches := s.descTree(treeID)
	require.Equal(s.T(), concurrency, len(branches))

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
			require.Nil(s.T(), err)
			// it shouldn't change anything
			events = s.read(branch, 1, 25)
			require.Equal(s.T(), 20, len(events))

			// override with greatest txn_id
			events = s.genRandomEvents([]int64{5}, 1)
			err = s.appendNewNode(branch, events, 3000)
			require.Nil(s.T(), err)

			// read to verify override success, at this point history is corrupted, missing 6/7/8, so we should only see 5 events
			events = s.read(branch, 1, 6)
			require.Equal(s.T(), 5, len(events))
			_, err = s.readWithError(branch, 1, 25)
			_, ok := err.(*serviceerror.DataLoss)
			require.Equal(s.T(), true, ok)

			// override with even larger txn_id and same version
			events = s.genRandomEvents([]int64{5, 6}, 1)
			err = s.appendNewNode(branch, events, 3001)
			require.Nil(s.T(), err)

			// read to verify override success, at this point history is corrupted, missing 7/8, so we should only see 6 events
			events = s.read(branch, 1, 7)
			require.Equal(s.T(), 6, len(events))
			_, err = s.readWithError(branch, 1, 25)
			_, ok = err.(*serviceerror.DataLoss)
			require.Equal(s.T(), true, ok)

			// override more with larger txn_id, this would fix the corrupted hole so that we cna get 20 events again
			events = s.genRandomEvents([]int64{7, 8}, 1)
			err = s.appendNewNode(branch, events, 3002)
			require.Nil(s.T(), err)

			// read to verify override
			events = s.read(branch, 1, 25)
			require.Equal(s.T(), 20, len(events))
			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}, 1)
			err = s.appendNewNode(branch, events, 4001)
			require.Nil(s.T(), err)
			events = s.read(branch, 1, 25)
			require.Equal(s.T(), 23, len(events))
		}(i)
	}

	wg.Wait()
	// Finally lets clean up all branches
	m.Range(func(k, v interface{}) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		require.Nil(s.T(), err)
		return true
	})

	branches = s.descTree(treeID)
	require.Equal(s.T(), 0, len(branches))
}

// TestConcurrentlyForkAndAppendBranches test
func (s *HistoryV2PersistenceSuite) TestConcurrentlyForkAndAppendBranches() {
	treeID := uuid.NewRandom().String()
	wg := sync.WaitGroup{}
	concurrency := 10
	masterBr, err := s.newHistoryBranch(treeID)
	require.Nil(s.T(), err)
	branches := s.descTree(treeID)
	require.Equal(s.T(), 0, len(branches))

	// append first batch to master branch
	eids := []int64{}
	for i := int64(1); i <= int64(concurrency)+1; i++ {
		eids = append(eids, i)
	}
	events := s.genRandomEvents(eids, 1)
	err = s.appendNewBranchAndFirstNode(masterBr, events[0:1], 1, "masterbr")
	require.Nil(s.T(), err)

	readEvents := s.read(masterBr, 1, int64(concurrency)+2)
	require.Nil(s.T(), err)
	require.Equal(s.T(), 1, len(readEvents))

	branches = s.descTree(treeID)
	require.Equal(s.T(), 1, len(branches))
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
	require.Nil(s.T(), err)
	events = s.read(masterBr, 1, int64(concurrency)+2)
	require.Nil(s.T(), err)
	require.Equal(s.T(), (concurrency)+1, len(events))

	level1ID := new(sync.Map)
	level1Br := new(sync.Map)
	// test forking from master branch and append nodes
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			forkNodeID := rand.Int63n(int64(concurrency)) + 2
			level1ID.Store(idx, forkNodeID)

			bi, err := s.fork(masterBr, forkNodeID)
			require.Nil(s.T(), err)
			level1Br.Store(idx, bi)

			// cannot append to ancestors
			events := s.genRandomEvents([]int64{forkNodeID - 1}, 1)
			err = s.appendNewNode(bi, events, reserveTxn(1))
			_, ok := err.(*p.InvalidPersistenceRequestError)
			require.Equal(s.T(), true, ok)

			// append second batch to first level
			eids := make([]int64, 0)
			for i := forkNodeID; i <= int64(concurrency)*2+1; i++ {
				eids = append(eids, i)
			}
			events = s.genRandomEvents(eids, 1)

			err = s.appendNewNode(bi, events[0:1], reserveTxn(1))
			require.Nil(s.T(), err)

			err = s.appendOneByOne(bi, events[1:], reserveTxn(len(events[1:])))
			require.Nil(s.T(), err)

			events = s.read(bi, 1, int64(concurrency)*2+2)
			require.Nil(s.T(), err)
			require.Equal(s.T(), (concurrency)*2+1, len(events))

			if idx == 0 {
				err = s.deleteHistoryBranch(bi)
				require.Nil(s.T(), err)
			}

		}(i)
	}

	wg.Wait()
	branches = s.descTree(treeID)
	require.Equal(s.T(), concurrency, len(branches))
	forkOnLevel1 := int32(0)
	level2Br := new(sync.Map)
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
			require.Nil(s.T(), err)
			level2Br.Store(idx, bi)

			// append second batch to second level
			eids := make([]int64, 0)
			for i := forkNodeID; i <= int64(concurrency)*3+1; i++ {
				eids = append(eids, i)
			}
			events := s.genRandomEvents(eids, 1)
			err = s.appendNewNode(bi, events[0:1], reserveTxn(1))
			require.Nil(s.T(), err)
			err = s.appendOneByOne(bi, events[1:], reserveTxn(len(events[1:])))
			require.Nil(s.T(), err)
			events = s.read(bi, 1, int64(concurrency)*3+2)
			require.Nil(s.T(), err)
			require.Equal(s.T(), (concurrency)*3+1, len(events))

			// try override last event
			events = s.genRandomEvents([]int64{int64(concurrency)*3 + 1}, 1)
			err = s.appendNewNode(bi, events, reserveTxn(1))
			require.Nil(s.T(), err)
			events = s.read(bi, 1, int64(concurrency)*3+2)
			require.Nil(s.T(), err)
			require.Equal(s.T(), (concurrency)*3+1, len(events))

			// test fork and newBranch concurrently
			bi, err = s.newHistoryBranch(treeID)
			require.Nil(s.T(), err)
			level2Br.Store(concurrency+idx, bi)

			events = s.genRandomEvents([]int64{1}, 1)
			err = s.appendNewBranchAndFirstNode(bi, events, reserveTxn(1), "newbr")
			require.Nil(s.T(), err)

		}(i)
	}

	wg.Wait()
	branches = s.descTree(treeID)
	require.Equal(s.T(), concurrency*3-2, len(branches))
	actualForkOnLevel1 := int32(0)
	masterCnt := 0
	for _, b := range branches {
		if len(b.Ancestors) == 2 {
			actualForkOnLevel1++
		} else if len(b.Ancestors) == 0 {
			masterCnt++
		} else {
			require.Equal(s.T(), 1, len(b.Ancestors))
			require.Equal(s.T(), mbrID, b.Ancestors[0].GetBranchId())
		}
	}
	require.Equal(s.T(), forkOnLevel1, actualForkOnLevel1)
	require.Equal(s.T(), concurrency, masterCnt)

	// Finally lets clean up all branches
	level1Br.Range(func(k, v interface{}) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		require.Nil(s.T(), err)

		return true
	})
	level2Br.Range(func(k, v interface{}) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		require.Nil(s.T(), err)

		return true
	})
	err = s.deleteHistoryBranch(masterBr)
	require.Nil(s.T(), err)

	branches = s.descTree(treeID)
	require.Equal(s.T(), 0, len(branches))

}

func (s *HistoryV2PersistenceSuite) getBranchByKey(m *sync.Map, k int) []byte {
	v, ok := m.Load(k)
	require.Equal(s.T(), true, ok)
	br := v.([]byte)
	return br
}

func (s *HistoryV2PersistenceSuite) getIDByKey(m *sync.Map, k int) int64 {
	v, ok := m.Load(k)
	require.Equal(s.T(), true, ok)
	id := v.(int64)
	return id
}

func (s *HistoryV2PersistenceSuite) genRandomEvents(eventIDs []int64, version int64) []*historypb.HistoryEvent {
	var events []*historypb.HistoryEvent

	now := time.Date(2020, 8, 22, 0, 0, 0, 0, time.UTC)
	for _, eid := range eventIDs {
		e := &historypb.HistoryEvent{EventId: eid, Version: version, EventTime: timestamppb.New(now)}
		events = append(events, e)
	}

	return events
}

// persistence helper
func (s *HistoryV2PersistenceSuite) newHistoryBranch(treeID string) ([]byte, error) {
	return s.ExecutionManager.GetHistoryBranchUtil().NewHistoryBranch(
		uuid.New(),
		uuid.New(),
		uuid.New(),
		treeID,
		nil,
		[]*persistencespb.HistoryBranchRange{},
		0,
		0,
		0,
	)
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
func (s *HistoryV2PersistenceSuite) descTree(treeID string) []*persistencespb.HistoryBranch {
	var branches []*persistencespb.HistoryBranch

	var nextPageToken []byte
	for {
		resp, err := s.ExecutionManager.GetAllHistoryTreeBranches(s.ctx, &p.GetAllHistoryTreeBranchesRequest{
			NextPageToken: nextPageToken,
			PageSize:      100,
		})
		require.NoError(s.T(), err)

		for _, branch := range resp.Branches {
			if branch.BranchInfo.TreeId == treeID {
				branches = append(branches, branch.BranchInfo)
			}
		}

		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
	}

	return branches
}

// persistence helper
func (s *HistoryV2PersistenceSuite) read(branch []byte, minID, maxID int64) []*historypb.HistoryEvent {
	res, err := s.readWithError(branch, minID, maxID)
	require.Nil(s.T(), err)
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
			require.True(s.T(), resp.Size > 0)
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
	require.True(s.T(), resp.Size > 0)

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
			NamespaceID:     uuid.New(),
			NewRunID:        uuid.New(),
		})
		if resp != nil {
			bi = resp.NewBranchToken
		}
		return err
	}

	err := backoff.ThrottleRetry(op, historyTestRetryPolicy, isConditionFail)
	return bi, err
}
