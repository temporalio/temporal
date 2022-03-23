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

package membership

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/temporalio/ringpop-go"
	"github.com/uber/tchannel-go"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

// TestRingpopCluster is a type that represents a test ringpop cluster
type TestRingpopCluster struct {
	hostUUIDs    []string
	hostAddrs    []string
	hostInfoList []HostInfo
	rings        []Monitor
	channels     []*tchannel.Channel
	seedNode     string
}

// NewTestRingpopCluster creates a new test cluster with the given name and cluster size
// All the nodes in the test cluster will register themselves in Ringpop
// with the specified name. This is only intended for unit tests.
func NewTestRingpopCluster(
	t *testing.T,
	ringPopApp string,
	size int,
	listenIPAddr string,
	seed string,
	serviceName string,
	broadcastAddress string,
) *TestRingpopCluster {
	logger := log.NewTestLogger()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := persistence.NewMockClusterMetadataManager(ctrl)
	mockMgr.EXPECT().PruneClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockMgr.EXPECT().UpsertClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cluster := &TestRingpopCluster{
		hostUUIDs:    make([]string, size),
		hostAddrs:    make([]string, size),
		hostInfoList: make([]HostInfo, size),
		rings:        make([]Monitor, size),
		channels:     make([]*tchannel.Channel, size),
		seedNode:     seed,
	}

	for i := 0; i < size; i++ {
		var err error
		cluster.channels[i], err = tchannel.NewChannel(ringPopApp, nil)
		if err != nil {
			logger.Error("Failed to create tchannel", tag.Error(err))
			return nil
		}
		listenAddr := listenIPAddr + ":0"
		err = cluster.channels[i].ListenAndServe(listenAddr)
		if err != nil {
			logger.Error("tchannel listen failed", tag.Error(err))
			return nil
		}
		cluster.hostUUIDs[i] = uuid.New()
		cluster.hostAddrs[i], err = BuildBroadcastHostPort(cluster.channels[i].PeerInfo(), broadcastAddress)
		if err != nil {
			logger.Error("Failed to build broadcast hostport", tag.Error(err))
			return nil
		}
		cluster.hostInfoList[i] = HostInfo{addr: cluster.hostAddrs[i]}
	}

	// if seed node is already supplied, use it; if not, set it
	if cluster.seedNode == "" {
		cluster.seedNode = cluster.hostAddrs[0]
	}
	logger.Info("seedNode", tag.Name(cluster.seedNode))

	seedAddress, seedPort, err := SplitHostPortTyped(cluster.seedNode)
	if err != nil {
		logger.Error("unable to split host port", tag.Error(err))
		return nil
	}
	seedMember := &persistence.ClusterMember{
		HostID:        uuid.NewUUID(),
		RPCAddress:    seedAddress,
		RPCPort:       seedPort,
		SessionStart:  time.Now().UTC(),
		LastHeartbeat: time.Now().UTC(),
	}

	firstGetClusterMemberCall := true
	mockMgr.EXPECT().GetClusterMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *persistence.GetClusterMembersRequest) (*persistence.GetClusterMembersResponse, error) {
			res := &persistence.GetClusterMembersResponse{ActiveMembers: []*persistence.ClusterMember{seedMember}}

			if firstGetClusterMemberCall {
				// The first time GetClusterMembers is invoked, we simulate returning a stale/bad heartbeat.
				// All subsequent calls only return the single "good" seed member
				// This ensures that we exercise the retry path in bootstrap properly.
				badSeedMember := &persistence.ClusterMember{
					HostID:        uuid.NewUUID(),
					RPCAddress:    seedAddress,
					RPCPort:       seedPort + 1,
					SessionStart:  time.Now().UTC(),
					LastHeartbeat: time.Now().UTC(),
				}
				res = &persistence.GetClusterMembersResponse{ActiveMembers: []*persistence.ClusterMember{seedMember, badSeedMember}}
			}

			firstGetClusterMemberCall = false
			return res, nil
		}).AnyTimes()

	for i := 0; i < size; i++ {
		resolver := func() (string, error) {
			return BuildBroadcastHostPort(cluster.channels[i].PeerInfo(), broadcastAddress)
		}

		ringPop, err := ringpop.New(ringPopApp, ringpop.Channel(cluster.channels[i]), ringpop.AddressResolverFunc(resolver))
		if err != nil {
			logger.Error("failed to create ringpop instance", tag.Error(err))
			return nil
		}
		rpWrapper := NewRingPop(ringPop, time.Second*2, logger)
		cluster.rings[i] = NewRingpopMonitor(
			serviceName,
			map[string]int{serviceName: 0},
			rpWrapper,
			logger,
			mockMgr,
			resolver,
		)
		cluster.rings[i].Start()
	}
	return cluster
}

// GetSeedNode returns the seedNode for this cluster
func (c *TestRingpopCluster) GetSeedNode() string {
	return c.seedNode
}

// KillHost kills the given host within the cluster
func (c *TestRingpopCluster) KillHost(uuid string) {
	for i := 0; i < len(c.hostUUIDs); i++ {
		if c.hostUUIDs[i] == uuid {
			c.rings[i].Stop()
			c.channels[i].Close()
			c.rings[i] = nil
			c.channels[i] = nil
		}
	}
}

// Stop stops the cluster
func (c *TestRingpopCluster) Stop() {
	for i := 0; i < len(c.hostAddrs); i++ {
		if c.rings[i] != nil {
			c.rings[i].Stop()
			c.channels[i].Close()
		}
	}
}

// GetHostInfoList returns the list of all hosts within the cluster
func (c *TestRingpopCluster) GetHostInfoList() []HostInfo {
	return c.hostInfoList
}

// GetHostAddrs returns all host addrs within the cluster
func (c *TestRingpopCluster) GetHostAddrs() []string {
	return c.hostAddrs
}

// FindHostByAddr returns the host info corresponding to
// the given addr, if it exists
func (c *TestRingpopCluster) FindHostByAddr(addr string) (HostInfo, bool) {
	for _, hi := range c.hostInfoList {
		if hi.addr == addr {
			return hi, true
		}
	}
	return HostInfo{}, false
}
