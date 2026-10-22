package persistencetests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/debug"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// HistoryV2PersistenceSuite contains history persistence tests
	HistoryV2PersistenceSuite struct {
		// suite.Suite
		*TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
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
	s.ProtoAssertions = protorequire.New(s.T())

	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)
}

// TearDownTest implementation
func (s *HistoryV2PersistenceSuite) TearDownTest() {
	s.cancel()
}

// TestGenUUIDs testing  uuid.NewString() can generate unique UUID
func (s *HistoryV2PersistenceSuite) TestGenUUIDs() {
	wg := sync.WaitGroup{}
	m := sync.Map{}
	concurrency := 1000
	for range concurrency {
		wg.Go(func() {
			u := uuid.NewString()
			m.Store(u, true)
		})
	}
	wg.Wait()
	cnt := 0
	m.Range(func(k, v any) bool {
		cnt++
		return true
	})
	s.Equal(concurrency, cnt)
}

// TestScanAllTrees test
func (s *HistoryV2PersistenceSuite) TestScanAllTrees() {
	resp, err := s.ExecutionManager.GetAllHistoryTreeBranches(s.ctx, &p.GetAllHistoryTreeBranchesRequest{
		PageSize: 1,
	})
	s.NoError(err)
	s.Empty(resp.Branches, "some trees were leaked in other tests")

	trees := map[string]bool{}
	totalTrees := 1002
	pgSize := 100

	for range totalTrees {
		treeID := uuid.NewString()
		bi, err := s.newHistoryBranch(treeID)
		s.NoError(err)

		events := s.genRandomEvents([]int64{1, 2, 3}, 1)
		err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
		s.NoError(err)
		trees[string(treeID)] = true
	}

	var pgToken []byte
	for {
		resp, err := s.ExecutionManager.GetAllHistoryTreeBranches(s.ctx, &p.GetAllHistoryTreeBranchesRequest{
			PageSize:      pgSize,
			NextPageToken: pgToken,
		})
		s.NoError(err)
		for _, br := range resp.Branches {
			uuidTreeID := br.BranchInfo.TreeId
			if trees[uuidTreeID] {
				delete(trees, uuidTreeID)

				s.Positive(br.ForkTime.AsTime().UnixNano())
				s.NotEmpty(br.BranchInfo.BranchId)
				s.Equal("branchInfo", br.Info)
			} else {
				s.Fail("treeID not found", br.BranchInfo.TreeId)
			}
		}

		if len(resp.NextPageToken) == 0 {
			break
		}
		pgToken = resp.NextPageToken
	}

	s.Empty(trees)
}

// TestReadBranchByPagination test
func (s *HistoryV2PersistenceSuite) TestReadBranchByPagination() {
	treeID := uuid.NewString()
	bi, err := s.newHistoryBranch(treeID)
	s.NoError(err)

	historyW := &historypb.History{}
	events := s.genRandomEvents([]int64{1, 2, 3}, 0)
	err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
	s.NoError(err)
	historyW.Events = events

	events = s.genRandomEvents([]int64{4}, 0)
	err = s.appendNewNode(bi, events, 2)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{5, 6, 7, 8}, 4)
	err = s.appendNewNode(bi, events, 6)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 1)
	err = s.appendNewNode(bi, events, 3)
	s.NoError(err)
	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 2)
	err = s.appendNewNode(bi, events, 4)
	s.NoError(err)
	// stale event batch
	events = s.genRandomEvents([]int64{6, 7, 8}, 3)
	err = s.appendNewNode(bi, events, 5)
	s.NoError(err)

	events = s.genRandomEvents([]int64{9}, 4)
	err = s.appendNewNode(bi, events, 7)
	s.NoError(err)
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
	s.NoError(err)
	s.Len(resp.HistoryEvents, 4)
	s.Equal(int64(6), resp.HistoryEvents[0].GetEventId())

	events = s.genRandomEvents([]int64{10}, 4)
	err = s.appendNewNode(bi, events, 8)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{11}, 4)
	err = s.appendNewNode(bi, events, 9)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{12}, 4)
	err = s.appendNewNode(bi, events, 10)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{13, 14, 15}, 4)
	err = s.appendNewNode(bi, events, 11)
	s.NoError(err)
	// we don't append this batch because we will fork from 13
	// historyW.Events = append(historyW.Events, events...)

	// fork from here
	bi2, err := s.fork(bi, 13)
	s.NoError(err)

	events = s.genRandomEvents([]int64{13}, 4)
	err = s.appendNewNode(bi2, events, 12)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{14}, 4)
	err = s.appendNewNode(bi2, events, 13)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{15, 16, 17}, 4)
	err = s.appendNewNode(bi2, events, 14)
	s.NoError(err)
	historyW.Events = append(historyW.Events, events...)

	events = s.genRandomEvents([]int64{18, 19, 20}, 4)
	err = s.appendNewNode(bi2, events, 15)
	s.NoError(err)
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
	s.NoError(err)

	s.Len(resp.HistoryEvents, 8)
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// this page is all stale batches
	// doe to difference in Cassandra / MySQL pagination
	// the stale event batch may get returned
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.NoError(err)
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken
	if len(resp.HistoryEvents) == 0 {
		// second page
		resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
		s.NoError(err)
		s.Len(resp.HistoryEvents, 3)
		historyR.Events = append(historyR.Events, resp.HistoryEvents...)
		req.NextPageToken = resp.NextPageToken
	} else if len(resp.HistoryEvents) == 3 {
		// no op
	} else {
		s.Fail("should either return 0 (Cassandra) or 3 (MySQL) events")
	}

	// 3rd page, since we fork from nodeID=13, we can only see one batch of 12 here
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.NoError(err)
	s.Len(resp.HistoryEvents, 1)
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// 4th page, 13~17
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.NoError(err)
	s.Len(resp.HistoryEvents, 5)
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken

	// last page: one batch of 18-20
	// We have only one page left and the page size is set to one. In this case,
	// persistence may or may not return a nextPageToken.
	// If it does return a token, we need to ensure that if the token returned is used
	// to get history again, no error and history events should be returned.
	req.PageSize = 1
	resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.NoError(err)
	s.Len(resp.HistoryEvents, 3)
	historyR.Events = append(historyR.Events, resp.HistoryEvents...)
	req.NextPageToken = resp.NextPageToken
	if len(resp.NextPageToken) != 0 {
		resp, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
		s.NoError(err)
		s.Empty(resp.HistoryEvents)
	}

	s.ProtoEqual(historyW, historyR)
	s.Empty(resp.NextPageToken)

	// MinEventID is in the middle of the last batch and this is the first request (NextPageToken
	// is empty), the call should return an error.
	req.MinEventID = 19
	req.NextPageToken = nil
	_, err = s.ExecutionManager.ReadHistoryBranch(s.ctx, req)
	s.ErrorAs(err, new(*serviceerror.NotFound))

	err = s.deleteHistoryBranch(bi2)
	s.NoError(err)
	err = s.deleteHistoryBranch(bi)
	s.NoError(err)
	branches := s.descTree(treeID)
	s.Empty(branches)
}

// TestConcurrentlyCreateAndAppendBranches test
func (s *HistoryV2PersistenceSuite) TestConcurrentlyCreateAndAppendBranches() {
	treeID := uuid.NewString()
	concurrency := 1
	m := &sync.Map{}
	writtenHistories := make([]*historypb.History, concurrency)
	readHistories := make([]*historypb.History, concurrency)

	// test create new branch along with appending new nodes
	var group errgroup.Group
	for idx := range concurrency {
		group.Go(func() error {
			bi, err := s.newHistoryBranch(treeID)
			if err != nil {
				return fmt.Errorf("create history branch: %w", err)
			}
			historyW := &historypb.History{}
			m.Store(idx, bi)

			events := s.genRandomEvents([]int64{1, 2, 3}, 1)
			err = s.appendNewBranchAndFirstNode(bi, events, 1, "branchInfo")
			if err != nil {
				return fmt.Errorf("append first history node: %w", err)
			}
			historyW.Events = events

			events = s.genRandomEvents([]int64{4}, 1)
			err = s.appendNewNode(bi, events, 2)
			if err != nil {
				return fmt.Errorf("append second history node: %w", err)
			}
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{5, 6, 7, 8}, 1)
			err = s.appendNewNode(bi, events, 3)
			if err != nil {
				return fmt.Errorf("append third history node: %w", err)
			}
			historyW.Events = append(historyW.Events, events...)

			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 1)
			err = s.appendNewNode(bi, events, 4000)
			if err != nil {
				return fmt.Errorf("append fourth history node: %w", err)
			}
			historyW.Events = append(historyW.Events, events...)

			// read branch to verify
			historyR := &historypb.History{}
			events, err = s.readWithError(bi, 1, 21)
			if err != nil {
				return fmt.Errorf("read history branch: %w", err)
			}
			if len(events) != 20 {
				return fmt.Errorf("read history branch: got %d events, want 20", len(events))
			}
			historyR.Events = events
			writtenHistories[idx] = historyW
			readHistories[idx] = historyR
			return nil
		})
	}

	s.NoError(group.Wait())
	for idx := range concurrency {
		s.ProtoEqual(writtenHistories[idx], readHistories[idx])
	}
	branches := s.descTree(treeID)
	s.Len(branches, concurrency)

	group = errgroup.Group{}
	// test appending nodes(override and new nodes) on each branch concurrently
	for idx := range concurrency {
		group.Go(func() error {
			branch, ok := s.getBranchByKey(m, idx)
			if !ok {
				return fmt.Errorf("history branch %d not found", idx)
			}

			// override with smaller txn_id
			events := s.genRandomEvents([]int64{5}, 1)
			err := s.appendNewNode(branch, events, 0)
			if err != nil {
				return fmt.Errorf("append history node with smaller transaction ID: %w", err)
			}
			// it shouldn't change anything
			events, err = s.readWithError(branch, 1, 25)
			if err != nil {
				return fmt.Errorf("read unchanged history: %w", err)
			}
			if len(events) != 20 {
				return fmt.Errorf("read unchanged history: got %d events, want 20", len(events))
			}

			// override with greatest txn_id
			events = s.genRandomEvents([]int64{5}, 1)
			err = s.appendNewNode(branch, events, 3000)
			if err != nil {
				return fmt.Errorf("append history node with greater transaction ID: %w", err)
			}

			// read to verify override success, at this point history is corrupted, missing 6/7/8, so we should only see 5 events
			events, err = s.readWithError(branch, 1, 6)
			if err != nil {
				return fmt.Errorf("read first overridden history: %w", err)
			}
			if len(events) != 5 {
				return fmt.Errorf("read first overridden history: got %d events, want 5", len(events))
			}
			_, err = s.readWithError(branch, 1, 25)
			if _, ok := errors.AsType[*serviceerror.DataLoss](err); !ok {
				return fmt.Errorf("read first corrupted history: got %T, want *serviceerror.DataLoss", err)
			}

			// override with even larger txn_id and same version
			events = s.genRandomEvents([]int64{5, 6}, 1)
			err = s.appendNewNode(branch, events, 3001)
			if err != nil {
				return fmt.Errorf("append history node with even larger transaction ID: %w", err)
			}

			// read to verify override success, at this point history is corrupted, missing 7/8, so we should only see 6 events
			events, err = s.readWithError(branch, 1, 7)
			if err != nil {
				return fmt.Errorf("read second overridden history: %w", err)
			}
			if len(events) != 6 {
				return fmt.Errorf("read second overridden history: got %d events, want 6", len(events))
			}
			_, err = s.readWithError(branch, 1, 25)
			if _, ok := errors.AsType[*serviceerror.DataLoss](err); !ok {
				return fmt.Errorf("read second corrupted history: got %T, want *serviceerror.DataLoss", err)
			}

			// override more with larger txn_id, this would fix the corrupted hole so that we cna get 20 events again
			events = s.genRandomEvents([]int64{7, 8}, 1)
			err = s.appendNewNode(branch, events, 3002)
			if err != nil {
				return fmt.Errorf("append history node to repair corrupted history: %w", err)
			}

			// read to verify override
			events, err = s.readWithError(branch, 1, 25)
			if err != nil {
				return fmt.Errorf("read repaired history: %w", err)
			}
			if len(events) != 20 {
				return fmt.Errorf("read repaired history: got %d events, want 20", len(events))
			}
			events = s.genRandomEvents([]int64{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}, 1)
			err = s.appendNewNode(branch, events, 4001)
			if err != nil {
				return fmt.Errorf("append final history node: %w", err)
			}
			events, err = s.readWithError(branch, 1, 25)
			if err != nil {
				return fmt.Errorf("read final history: %w", err)
			}
			if len(events) != 23 {
				return fmt.Errorf("read final history: got %d events, want 23", len(events))
			}
			return nil
		})
	}

	s.NoError(group.Wait())
	// Finally lets clean up all branches
	m.Range(func(k, v any) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		s.NoError(err)
		return true
	})

	branches = s.descTree(treeID)
	s.Empty(branches)
}

// TestConcurrentlyForkAndAppendBranches test
func (s *HistoryV2PersistenceSuite) TestConcurrentlyForkAndAppendBranches() {
	treeID := uuid.NewString()
	concurrency := 10
	masterBr, err := s.newHistoryBranch(treeID)
	s.NoError(err)
	branches := s.descTree(treeID)
	s.Empty(branches)

	// append first batch to master branch
	eids := []int64{}
	for i := int64(1); i <= int64(concurrency)+1; i++ {
		eids = append(eids, i)
	}
	events := s.genRandomEvents(eids, 1)
	err = s.appendNewBranchAndFirstNode(masterBr, events[0:1], 1, "masterbr")
	s.NoError(err)

	readEvents := s.read(masterBr, 1, int64(concurrency)+2)
	s.Len(readEvents, 1)

	branches = s.descTree(treeID)
	s.Len(branches, 1)
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
	s.NoError(err)
	events = s.read(masterBr, 1, int64(concurrency)+2)
	s.Len(events, (concurrency)+1)

	level1ID := new(sync.Map)
	level1Br := new(sync.Map)
	// test forking from master branch and append nodes
	var group errgroup.Group
	for idx := range concurrency {
		group.Go(func() error {
			forkNodeID := rand.Int63n(int64(concurrency)) + 2
			level1ID.Store(idx, forkNodeID)

			bi, err := s.fork(masterBr, forkNodeID)
			if err != nil {
				return fmt.Errorf("fork first-level history branch: %w", err)
			}
			level1Br.Store(idx, bi)

			// cannot append to ancestors
			events := s.genRandomEvents([]int64{forkNodeID - 1}, 1)
			err = s.appendNewNode(bi, events, reserveTxn(1))
			if _, ok := errors.AsType[*p.InvalidPersistenceRequestError](err); !ok {
				return fmt.Errorf("append to ancestor: got %T, want *persistence.InvalidPersistenceRequestError", err)
			}

			// append second batch to first level
			eids := make([]int64, 0)
			for i := forkNodeID; i <= int64(concurrency)*2+1; i++ {
				eids = append(eids, i)
			}
			events = s.genRandomEvents(eids, 1)

			err = s.appendNewNode(bi, events[0:1], reserveTxn(1))
			if err != nil {
				return fmt.Errorf("append first node to first-level branch: %w", err)
			}

			err = s.appendOneByOne(bi, events[1:], reserveTxn(len(events[1:])))
			if err != nil {
				return fmt.Errorf("append remaining nodes to first-level branch: %w", err)
			}

			events, err = s.readWithError(bi, 1, int64(concurrency)*2+2)
			if err != nil {
				return fmt.Errorf("read first-level history branch: %w", err)
			}
			if len(events) != concurrency*2+1 {
				return fmt.Errorf("read first-level history branch: got %d events, want %d", len(events), concurrency*2+1)
			}

			if idx == 0 {
				err = s.deleteHistoryBranch(bi)
				if err != nil {
					return fmt.Errorf("delete first-level history branch: %w", err)
				}
			}

			return nil
		})
	}

	s.NoError(group.Wait())
	branches = s.descTree(treeID)
	s.Len(branches, concurrency)
	forkOnLevel1 := int32(0)
	level2Br := new(sync.Map)
	group = errgroup.Group{}

	// test forking for second level of branch
	for idx := 1; idx < concurrency; idx++ {
		group.Go(func() error {
			// Event we fork from level1 branch, it is possible that the new branch will fork from master branch
			forkNodeID := rand.Int63n(int64(concurrency)*2) + 2
			forkBr, ok := s.getBranchByKey(level1Br, idx)
			if !ok {
				return fmt.Errorf("first-level history branch %d not found", idx)
			}
			lastForkNodeID, ok := s.getIDByKey(level1ID, idx)
			if !ok {
				return fmt.Errorf("first-level fork node ID %d not found", idx)
			}

			if forkNodeID > lastForkNodeID {
				atomic.AddInt32(&forkOnLevel1, int32(1))
			}

			bi, err := s.fork(forkBr, forkNodeID)
			if err != nil {
				return fmt.Errorf("fork second-level history branch: %w", err)
			}
			level2Br.Store(idx, bi)

			// append second batch to second level
			eids := make([]int64, 0)
			for i := forkNodeID; i <= int64(concurrency)*3+1; i++ {
				eids = append(eids, i)
			}
			events := s.genRandomEvents(eids, 1)
			err = s.appendNewNode(bi, events[0:1], reserveTxn(1))
			if err != nil {
				return fmt.Errorf("append first node to second-level branch: %w", err)
			}
			err = s.appendOneByOne(bi, events[1:], reserveTxn(len(events[1:])))
			if err != nil {
				return fmt.Errorf("append remaining nodes to second-level branch: %w", err)
			}
			events, err = s.readWithError(bi, 1, int64(concurrency)*3+2)
			if err != nil {
				return fmt.Errorf("read second-level history branch: %w", err)
			}
			if len(events) != concurrency*3+1 {
				return fmt.Errorf("read second-level history branch: got %d events, want %d", len(events), concurrency*3+1)
			}

			// try override last event
			events = s.genRandomEvents([]int64{int64(concurrency)*3 + 1}, 1)
			err = s.appendNewNode(bi, events, reserveTxn(1))
			if err != nil {
				return fmt.Errorf("override final second-level history event: %w", err)
			}
			events, err = s.readWithError(bi, 1, int64(concurrency)*3+2)
			if err != nil {
				return fmt.Errorf("read overridden second-level history branch: %w", err)
			}
			if len(events) != concurrency*3+1 {
				return fmt.Errorf("read overridden second-level history branch: got %d events, want %d", len(events), concurrency*3+1)
			}

			// test fork and newBranch concurrently
			bi, err = s.newHistoryBranch(treeID)
			if err != nil {
				return fmt.Errorf("create concurrent history branch: %w", err)
			}
			level2Br.Store(concurrency+idx, bi)

			events = s.genRandomEvents([]int64{1}, 1)
			err = s.appendNewBranchAndFirstNode(bi, events, reserveTxn(1), "newbr")
			if err != nil {
				return fmt.Errorf("append first node to concurrent history branch: %w", err)
			}

			return nil
		})
	}

	s.NoError(group.Wait())
	branches = s.descTree(treeID)
	s.Len(branches, concurrency*3-2)
	actualForkOnLevel1 := int32(0)
	masterCnt := 0
	for _, b := range branches {
		if len(b.Ancestors) == 2 {
			actualForkOnLevel1++
		} else if len(b.Ancestors) == 0 {
			masterCnt++
		} else {
			s.Len(b.Ancestors, 1)
			s.Equal(mbrID, b.Ancestors[0].GetBranchId())
		}
	}
	s.Equal(forkOnLevel1, actualForkOnLevel1)
	s.Equal(concurrency, masterCnt)

	// Finally lets clean up all branches
	level1Br.Range(func(k, v any) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		s.NoError(err)

		return true
	})
	level2Br.Range(func(k, v any) bool {
		br := v.([]byte)
		// delete old branches along with create new branches
		err := s.deleteHistoryBranch(br)
		s.NoError(err)

		return true
	})
	err = s.deleteHistoryBranch(masterBr)
	s.NoError(err)

	branches = s.descTree(treeID)
	s.Empty(branches)

}

func (s *HistoryV2PersistenceSuite) getBranchByKey(m *sync.Map, k int) ([]byte, bool) {
	v, ok := m.Load(k)
	if !ok {
		return nil, false
	}
	br, ok := v.([]byte)
	return br, ok
}

func (s *HistoryV2PersistenceSuite) getIDByKey(m *sync.Map, k int) (int64, bool) {
	v, ok := m.Load(k)
	if !ok {
		return 0, false
	}
	id, ok := v.(int64)
	return id, ok
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
		uuid.NewString(),
		uuid.NewString(),
		uuid.NewString(),
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
		s.NoError(err)

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
	s.NoError(err)
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
		if len(resp.HistoryEvents) > 0 && resp.Size <= 0 {
			return nil, fmt.Errorf("history response size must be positive: %d", resp.Size)
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
	if resp.Size <= 0 {
		return fmt.Errorf("append response size must be positive: %d", resp.Size)
	}

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
			NamespaceID:     uuid.NewString(),
			NewRunID:        uuid.NewString(),
		})
		if resp != nil {
			bi = resp.NewBranchToken
		}
		return err
	}

	err := backoff.ThrottleRetry(op, historyTestRetryPolicy, isConditionFail)
	return bi, err
}
