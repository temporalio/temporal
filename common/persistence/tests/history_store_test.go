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
	"math/rand"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common/primitives/timestamp"

	"go.temporal.io/server/common"

	"go.temporal.io/server/common/dynamicconfig"

	"go.temporal.io/server/common/log"

	p "go.temporal.io/server/common/persistence"
)

// TODO add UT for the following
//  * DeleteHistoryBranch
//  * GetHistoryTree
//  * GetAllHistoryTreeBranches

type (
	historyEventsPacket struct {
		nodeID            int64
		transactionID     int64
		prevTransactionID int64
		events            []*historypb.HistoryEvent
	}

	historyEventsSuite struct {
		suite.Suite
		*require.Assertions

		store  p.HistoryManager
		logger log.Logger
	}
)

func newHistoryEventsSuite(
	t *testing.T,
	store p.HistoryStore,
	logger log.Logger,
) *historyEventsSuite {
	return &historyEventsSuite{
		Assertions: require.New(t),
		store: p.NewHistoryV2ManagerImpl(
			store,
			logger,
			dynamicconfig.GetIntPropertyFn(4*1024*1024),
		),
		logger: logger,
	}
}

func (s *historyEventsSuite) SetupSuite() {

}

func (s *historyEventsSuite) TearDownSuite() {

}

func (s *historyEventsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyEventsSuite) TearDownTest() {

}

func (s *historyEventsSuite) TestAppendSelect_First() {
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

	s.Equal(eventsPacket.events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *historyEventsSuite) TestAppendSelect_NonShadowing() {
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

	s.Equal(events, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *historyEventsSuite) TestAppendSelect_Shadowing() {
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

	s.Equal(events1, s.listAllHistoryEvents(shardID, branchToken))
}

func (s *historyEventsSuite) TestAppendForkSelect_NoShadowing() {
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

	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *historyEventsSuite) TestAppendForkSelect_Shadowing_NonLastBranch() {
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

	s.Equal(events0, s.listAllHistoryEvents(shardID, branchToken))
	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *historyEventsSuite) TestAppendForkSelect_Shadowing_LastBranch() {
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

	s.Equal(events0, s.listAllHistoryEvents(shardID, newBranchToken))

	eventsPacket21 := s.newHistoryEvents(
		[]int64{4, 5},
		eventsPacket0.transactionID+3,
		eventsPacket0.transactionID,
	)
	s.appendHistoryEvents(shardID, newBranchToken, eventsPacket21)
	events1 = append(events1, eventsPacket21.events...)

	s.Equal(events1, s.listAllHistoryEvents(shardID, newBranchToken))
}

func (s *historyEventsSuite) TestAppendSelectTrim() {
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

func (s *historyEventsSuite) TestAppendForkSelectTrim_NonLastBranch() {
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

func (s *historyEventsSuite) TestAppendForkSelectTrim_LastBranch() {
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

func (s *historyEventsSuite) appendHistoryEvents(
	shardID int32,
	branchToken []byte,
	packet historyEventsPacket,
) {
	_, err := s.store.AppendHistoryNodes(&p.AppendHistoryNodesRequest{
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

func (s *historyEventsSuite) forkHistoryBranch(
	shardID int32,
	branchToken []byte,
	newNodeID int64,
) []byte {
	resp, err := s.store.ForkHistoryBranch(&p.ForkHistoryBranchRequest{
		ShardID:         shardID,
		ForkBranchToken: branchToken,
		ForkNodeID:      newNodeID,
		Info:            "",
	})
	s.NoError(err)
	return resp.NewBranchToken
}

func (s *historyEventsSuite) trimHistoryBranch(
	shardID int32,
	branchToken []byte,
	nodeID int64,
	transactionID int64,
) {
	_, err := s.store.TrimHistoryBranch(&p.TrimHistoryBranchRequest{
		ShardID:       shardID,
		BranchToken:   branchToken,
		NodeID:        nodeID,
		TransactionID: transactionID,
	})
	s.NoError(err)
}

func (s *historyEventsSuite) listAllHistoryEvents(
	shardID int32,
	branchToken []byte,
) []*historypb.HistoryEvent {
	var token []byte
	var events []*historypb.HistoryEvent
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		resp, err := s.store.ReadHistoryBranch(&p.ReadHistoryBranchRequest{
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

func (s *historyEventsSuite) newHistoryEvents(
	eventIDs []int64,
	transactionID int64,
	prevTransactionID int64,
) historyEventsPacket {

	events := make([]*historypb.HistoryEvent, len(eventIDs))
	for index, eventID := range eventIDs {
		events[index] = &historypb.HistoryEvent{
			EventId:   eventID,
			EventTime: timestamp.TimePtr(time.Unix(0, rand.Int63()).UTC()),
		}
	}

	return historyEventsPacket{
		nodeID:            eventIDs[0],
		transactionID:     transactionID,
		prevTransactionID: prevTransactionID,
		events:            events,
	}
}
