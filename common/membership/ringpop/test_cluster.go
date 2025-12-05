package ringpop

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/tchannel-go"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

// testCluster is a type that represents a test ringpop cluster
type testCluster struct {
	hostUUIDs    []string
	hostAddrs    []string
	hostInfoList []membership.HostInfo
	rings        []*monitor
	channels     []*tchannel.Channel
	seedNode     string
}

// newTestCluster creates a new test cluster with the given name and cluster size
// All the nodes in the test cluster will register themselves in Ringpop
// with the specified name. This is only intended for unit tests.
func newTestCluster(
	t *testing.T,
	ringPopApp string,
	size int,
	listenIPAddr string,
	seed string,
	serviceName primitives.ServiceName,
	broadcastAddress string,
	joinTimes []time.Time,
	testInitialBootstrapFailure bool,
) *testCluster {
	logger := log.NewTestLogger()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := persistence.NewMockClusterMetadataManager(ctrl)
	mockMgr.EXPECT().PruneClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockMgr.EXPECT().UpsertClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cluster := &testCluster{
		hostUUIDs:    make([]string, size),
		hostAddrs:    make([]string, size),
		hostInfoList: make([]membership.HostInfo, size),
		rings:        make([]*monitor, size),
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
		cluster.hostUUIDs[i] = uuid.NewString()
		cluster.hostAddrs[i], err = buildBroadcastHostPort(cluster.channels[i].PeerInfo(), broadcastAddress)
		if err != nil {
			logger.Error("Failed to build broadcast hostport", tag.Error(err))
			return nil
		}
		cluster.hostInfoList[i] = newHostInfo(cluster.hostAddrs[i], nil)
	}

	// if seed node is already supplied, use it; if not, set it
	if cluster.seedNode == "" {
		cluster.seedNode = cluster.hostAddrs[0]
	}
	logger.Info("seedNode", tag.Name(cluster.seedNode))

	seedAddress, seedPort, err := splitHostPortTyped(cluster.seedNode)
	if err != nil {
		logger.Error("unable to split host port", tag.Error(err))
		return nil
	}
	// MarshalBinary never fails for UUIDs
	hostID, _ := uuid.New().MarshalBinary()

	seedMember := &persistence.ClusterMember{
		HostID:        hostID,
		RPCAddress:    seedAddress,
		RPCPort:       seedPort,
		SessionStart:  time.Now().UTC(),
		LastHeartbeat: time.Now().UTC(),
	}

	firstGetClusterMemberCall := testInitialBootstrapFailure
	mockMgr.EXPECT().GetClusterMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *persistence.GetClusterMembersRequest) (*persistence.GetClusterMembersResponse, error) {
			res := &persistence.GetClusterMembersResponse{ActiveMembers: []*persistence.ClusterMember{seedMember}}

			hostID, _ := uuid.New().MarshalBinary()
			if firstGetClusterMemberCall {
				// The first time GetClusterMembers is invoked, we simulate returning a stale/bad heartbeat.
				// All subsequent calls only return the single "good" seed member
				// This ensures that we exercise the retry path in bootstrap properly.
				badSeedMember := &persistence.ClusterMember{
					HostID:        hostID,
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
		node := i
		resolver := func() (string, error) {
			return buildBroadcastHostPort(cluster.channels[node].PeerInfo(), broadcastAddress)
		}

		ringPop, err := ringpop.New(ringPopApp, ringpop.Channel(cluster.channels[i]), ringpop.AddressResolverFunc(resolver))
		if err != nil {
			logger.Error("failed to create ringpop instance", tag.Error(err))
			return nil
		}
		_, port, _ := splitHostPortTyped(cluster.hostAddrs[i])
		var joinTime time.Time
		if i < len(joinTimes) {
			joinTime = joinTimes[i]
		}
		cluster.rings[i] = newMonitor(
			serviceName,
			config.ServicePortMap{serviceName: int(port)}, // use same port for "grpc" port
			ringPop,
			logger,
			mockMgr,
			resolver,
			2*time.Second,
			3*time.Second,
			joinTime,
		)
		cluster.rings[i].Start()
	}
	return cluster
}

// GetSeedNode returns the seedNode for this cluster
func (c *testCluster) GetSeedNode() string {
	return c.seedNode
}

// DrainHost marks the given host draining
func (c *testCluster) DrainHost(hostID string) {
	for i, uuid := range c.hostUUIDs {
		if uuid == hostID {
			c.rings[i].SetDraining(true)
		}
	}
}

// KillHost kills the given host within the cluster
func (c *testCluster) KillHost(hostID string) {
	for i := 0; i < len(c.hostUUIDs); i++ {
		if c.hostUUIDs[i] == hostID {
			c.rings[i].Stop()
			c.channels[i].Close()
			c.rings[i] = nil
			c.channels[i] = nil
		}
	}
}

// Stop stops the cluster
func (c *testCluster) Stop() {
	for i := 0; i < len(c.hostAddrs); i++ {
		if c.rings[i] != nil {
			c.rings[i].Stop()
			c.channels[i].Close()
		}
	}
}

// GetHostInfoList returns the list of all hosts within the cluster
func (c *testCluster) GetHostInfoList() []membership.HostInfo {
	return c.hostInfoList
}

// GetHostAddrs returns all host addrs within the cluster
func (c *testCluster) GetHostAddrs() []string {
	return c.hostAddrs
}

// FindHostByAddr returns the host info corresponding to
// the given addr, if it exists
func (c *testCluster) FindHostByAddr(addr string) (membership.HostInfo, bool) {
	for _, hi := range c.hostInfoList {
		if hi.GetAddress() == addr {
			return hi, true
		}
	}
	return nil, false
}
