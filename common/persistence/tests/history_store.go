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

package tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

// TODO add UT for the following
//  * DeleteHistoryBranch
//  * GetHistoryTree
//  * GetAllHistoryTreeBranches

type (
	HistoryEventsPacket struct {
		nodeID            int64
		transactionID     int64
		prevTransactionID int64
		events            []*historypb.HistoryEvent
	}

	HistoryEventsSuite struct {
		suite.Suite
		*require.Assertions

		store      p.ExecutionManager
		serializer serialization.Serializer
		logger     log.Logger

		Ctx    context.Context
		Cancel context.CancelFunc
	}
)

func NewHistoryEventsSuite(
	t *testing.T,
	store p.ExecutionStore,
	logger log.Logger,
) *HistoryEventsSuite {
	eventSerializer := serialization.NewSerializer()
	return &HistoryEventsSuite{
		Assertions: require.New(t),
		store: p.NewExecutionManager(
			store,
			eventSerializer,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		serializer: eventSerializer,
		logger:     logger,
	}
}

func (s *HistoryEventsSuite) SetupSuite() {

}

func (s *HistoryEventsSuite) TearDownSuite() {

}

func (s *HistoryEventsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), time.Second*30)
}

func (s *HistoryEventsSuite) TearDownTest() {
	s.Cancel()
}

func (s *HistoryEventsSuite) TestAppendSelect_First() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)

	eventsPacket := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket)

	s.Equal(eventsPacket.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket.events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendSelect_NonShadowing() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events = append(events, eventsPacket0.events...)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events = append(events, eventsPacket1.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendSelect_Shadowing() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	eventsPacket10 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket10)
	events0 = append(events0, eventsPacket10.events...)

	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))

	eventsPacket11 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket11)
	events1 = append(events1, eventsPacket11.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket11.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(events1, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelect_NoShadowing() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	eventsPacket10 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket10)
	events0 = append(events0, eventsPacket10.events...)

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 4)
	eventsPacket11 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket11)
	events1 = append(events1, eventsPacket11.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket10.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(eventsPacket11.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelect_Shadowing_NonLastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events0 = append(events0, eventsPacket1.events...)
	events1 = append(events1, eventsPacket1.events...)

	eventsPacket20 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+1,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket20)
	events0 = append(events0, eventsPacket20.events...)

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 6)
	eventsPacket21 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+2,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket21)
	events1 = append(events1, eventsPacket21.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, branchToken, 4, 6))
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(eventsPacket20.events, s.listHistoryEvents(shardID, branchToken, 6, 7))
	s.Equal(eventsPacket21.events, s.listHistoryEvents(shardID, newBranchToken, 6, 7))
	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelect_Shadowing_LastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 4)
	eventsPacket20 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket20)
	events0 = append(events0, eventsPacket20.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket20.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(events0, s.listAllHistoryEvents(shardID, newBranchToken))

	eventsPacket21 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+3,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket21)
	events1 = append(events1, eventsPacket21.events...)

	s.Equal(eventsPacket0.events, s.listHistoryEvents(shardID, newBranchToken, common.FirstEventID, 4))
	s.Equal(eventsPacket21.events, s.listHistoryEvents(shardID, newBranchToken, 4, 6))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendSelectTrim() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events = append(events, eventsPacket0.events...)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events = append(events, eventsPacket1.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	))

	s.trimHistoryBranch(shardID, branchToken, eventsPacket1.nodeID, eventsPacket1.transactionID)

	s.Equal(events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelectTrim_NonLastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events0 []*historypb.HistoryEvent
	var events1 []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events0 = append(events0, eventsPacket0.events...)
	events1 = append(events1, eventsPacket0.events...)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket1)
	events0 = append(events0, eventsPacket1.events...)
	events1 = append(events1, eventsPacket1.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	))

	eventsPacket20 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+2,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket20)
	events0 = append(events0, eventsPacket20.events...)

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 6)
	eventsPacket21 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket1.transactionID+3,
		eventsPacket1.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket21)
	events1 = append(events1, eventsPacket21.events...)

	if rand.Intn(2)%2 == 0 {
		s.trimHistoryBranch(shardID, branchToken, eventsPacket20.nodeID, eventsPacket20.transactionID)
	} else {
		s.trimHistoryBranch(shardID, newBranchToken, eventsPacket21.nodeID, eventsPacket21.transactionID)
	}

	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendForkSelectTrim_LastBranch() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)
	var events []*historypb.HistoryEvent

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, branchToken, eventsPacket0)
	events = append(events, eventsPacket0.events...)

	s.appendHistoryEvents(shardID, branchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	newBranchToken := s.forkHistoryBranch(shardID, branchToken, 4)
	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket1)
	events = append(events, eventsPacket1.events...)

	s.appendHistoryEvents(shardID, newBranchToken, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+3,
		eventsPacket0.transactionID,
	))

	s.trimHistoryBranch(shardID, newBranchToken, eventsPacket1.nodeID, eventsPacket1.transactionID)

	s.Equal(events, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *HistoryEventsSuite) TestAppendBatches() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	branchToken, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)

	eventsPacket1 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	eventsPacket2 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket1.transactionID+100,
		eventsPacket1.transactionID,
	)
	eventsPacket3 := s.newHistoryEvents(
		[]int64{6},
		eventsPacket2.transactionID+100,
		eventsPacket2.transactionID,
	)

	s.appendRawHistoryBatches(shardID, branchToken, eventsPacket1)
	s.appendRawHistoryBatches(shardID, branchToken, eventsPacket2)
	s.appendRawHistoryBatches(shardID, branchToken, eventsPacket3)
	s.Equal(eventsPacket1.events, s.listHistoryEvents(shardID, branchToken, common.FirstEventID, 4))
	expectedEvents := append(eventsPacket1.events, append(eventsPacket2.events, eventsPacket3.events...)...)
	events := s.listAllHistoryEvents(shardID, branchToken)
	s.Equal(expectedEvents, events)
}

func (s *HistoryEventsSuite) TestForkDeleteBranch_DeleteBaseBranchFirst() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	br1Token, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)

	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		rand.Int63(),
		0,
	)
	s.appendHistoryEvents(shardID, br1Token, eventsPacket0)

	s.appendHistoryEvents(shardID, br1Token, s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+1,
		eventsPacket0.transactionID,
	))

	br2Token := s.forkHistoryBranch(shardID, br1Token, 4)
	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+2,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, br2Token, eventsPacket1)

	// delete branch1, should only delete branch1:[4,5], keep branch1:[1,2,3] as it is used as ancestor by branch2
	s.deleteHistoryBranch(shardID, br1Token)
	// verify branch1:[1,2,3] still remains
	s.Equal(eventsPacket0.events, s.listAllHistoryEvents(shardID, br1Token))
	// verify branch2 is not affected
	s.Equal(append(eventsPacket0.events, eventsPacket1.events...), s.listAllHistoryEvents(shardID, br2Token))

	// delete branch2, should delete branch2:[4,5], and also should delete ancestor branch1:[1,2,3] as it is no longer
	// used by anyone
	s.deleteHistoryBranch(shardID, br2Token)

	// at this point, both branch1 and branch2 are deleted.
	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br1Token,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")

	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br2Token,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")
}

func (s *HistoryEventsSuite) TestForkDeleteBranch_DeleteForkedBranchFirst() {
	shardID := rand.Int31()
	treeID := uuid.New()
	branchID := uuid.New()
	br1Token, err := p.NewHistoryBranchTokenByBranchID(treeID, branchID)
	s.NoError(err)

	transactionID := rand.Int63()
	eventsPacket0 := s.newHistoryEvents(
		[]int64{1, 2, 3},
		transactionID,
		0,
	)
	s.appendHistoryEvents(shardID, br1Token, eventsPacket0)
	eventsPacket1 := s.newHistoryEvents(
		[]int64{4, 5},
		transactionID+1,
		transactionID,
	)
	s.appendHistoryEvents(shardID, br1Token, eventsPacket1)

	br2Token := s.forkHistoryBranch(shardID, br1Token, 4)
	s.appendHistoryEvents(shardID, br2Token, s.newHistoryEvents(
		[]int64{4, 5},
		transactionID+2,
		transactionID,
	))

	// delete branch2, should only delete branch2:[4,5], keep branch1:[1,2,3] [4,5] as it is by branch1
	s.deleteHistoryBranch(shardID, br2Token)
	// verify branch1 is not affected
	s.Equal(append(eventsPacket0.events, eventsPacket1.events...), s.listAllHistoryEvents(shardID, br1Token))

	// branch2:[4,5] should be deleted
	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br2Token,
		MinEventID:  4,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")

	// delete branch1, should delete branch1:[1,2,3] [4,5]
	s.deleteHistoryBranch(shardID, br1Token)

	// branch1 should be deleted
	_, err = s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: br1Token,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.LastEventID,
		PageSize:    1,
	})
	s.Error(err, "Workflow execution history not found.")
}

func (s *HistoryEventsSuite) appendHistoryEvents(
	shardID int32,
	branchToken []byte,
	packet HistoryEventsPacket,
) {
	_, err := s.store.AppendHistoryNodes(s.Ctx, &p.AppendHistoryNodesRequest{
		ShardID:           shardID,
		BranchToken:       branchToken,
		Events:            packet.events,
		TransactionID:     packet.transactionID,
		PrevTransactionID: packet.prevTransactionID,
		IsNewBranch:       packet.nodeID == common.FirstEventID,
		Info:              "",
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) appendRawHistoryBatches(
	shardID int32,
	branchToken []byte,
	packet HistoryEventsPacket,
) {
	blob, err := s.serializer.SerializeEvents(packet.events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	_, err = s.store.AppendRawHistoryNodes(s.Ctx, &p.AppendRawHistoryNodesRequest{
		ShardID:           shardID,
		BranchToken:       branchToken,
		NodeID:            packet.nodeID,
		TransactionID:     packet.transactionID,
		PrevTransactionID: packet.prevTransactionID,
		IsNewBranch:       packet.nodeID == common.FirstEventID,
		Info:              "",
		History:           blob,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) forkHistoryBranch(
	shardID int32,
	branchToken []byte,
	newNodeID int64,
) []byte {
	resp, err := s.store.ForkHistoryBranch(s.Ctx, &p.ForkHistoryBranchRequest{
		ShardID:         shardID,
		ForkBranchToken: branchToken,
		ForkNodeID:      newNodeID,
		Info:            "",
	})
	s.NoError(err)
	return resp.NewBranchToken
}

func (s *HistoryEventsSuite) deleteHistoryBranch(
	shardID int32,
	branchToken []byte,
) {
	err := s.store.DeleteHistoryBranch(s.Ctx, &p.DeleteHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) trimHistoryBranch(
	shardID int32,
	branchToken []byte,
	nodeID int64,
	transactionID int64,
) {
	_, err := s.store.TrimHistoryBranch(s.Ctx, &p.TrimHistoryBranchRequest{
		ShardID:       shardID,
		BranchToken:   branchToken,
		NodeID:        nodeID,
		TransactionID: transactionID,
	})
	s.NoError(err)
}

func (s *HistoryEventsSuite) listHistoryEvents(
	shardID int32,
	branchToken []byte,
	startEventID int64,
	endEventID int64,
) []*historypb.HistoryEvent {
	var token []byte
	var events []*historypb.HistoryEvent
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		resp, err := s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
			ShardID:       shardID,
			BranchToken:   branchToken,
			MinEventID:    startEventID,
			MaxEventID:    endEventID,
			PageSize:      1, // use 1 here for better testing exp
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		events = append(events, resp.HistoryEvents...)
	}
	return events
}

func (s *HistoryEventsSuite) listAllHistoryEvents(
	shardID int32,
	branchToken []byte,
) []*historypb.HistoryEvent {
	var token []byte
	var events []*historypb.HistoryEvent
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		resp, err := s.store.ReadHistoryBranch(s.Ctx, &p.ReadHistoryBranchRequest{
			ShardID:       shardID,
			BranchToken:   branchToken,
			MinEventID:    common.FirstEventID,
			MaxEventID:    common.LastEventID,
			PageSize:      1, // use 1 here for better testing exp
			NextPageToken: token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		events = append(events, resp.HistoryEvents...)
	}
	return events
}

func (s *HistoryEventsSuite) newHistoryEvents(
	eventIDs []int64,
	transactionID int64,
	prevTransactionID int64,
) HistoryEventsPacket {

	events := make([]*historypb.HistoryEvent, len(eventIDs))
	for index, eventID := range eventIDs {
		events[index] = &historypb.HistoryEvent{
			EventId:   eventID,
			EventTime: timestamp.TimePtr(time.Unix(0, rand.Int63()).UTC()),
		}
	}

	return HistoryEventsPacket{
		nodeID:            eventIDs[0],
		transactionID:     transactionID,
		prevTransactionID: prevTransactionID,
		events:            events,
	}
}
