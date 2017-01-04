package membership

import (
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	"github.com/uber/tchannel-go"
)

// TagErr is the tag for error object message
// TODO: move to constants file under common logging
const TagErr = `err`

// TestRingpopCluster is a type that represents a test ringpop cluster
type TestRingpopCluster struct {
	hostUUIDs    []string
	hostAddrs    []string
	hostInfoList []HostInfo
	rings        []*ringpop.Ringpop
	channels     []*tchannel.Channel
	seedNode     string
}

// NewTestRingpopCluster creates a new test cluster with the given name and cluster size
// All the nodes in the test cluster will register themselves in Ringpop
// with the specified name. This is only intended for unit tests.
func NewTestRingpopCluster(name string, size int, ipAddr string, seed string, sName string) *TestRingpopCluster {

	cluster := &TestRingpopCluster{
		hostUUIDs:    make([]string, size),
		hostAddrs:    make([]string, size),
		hostInfoList: make([]HostInfo, size),
		rings:        make([]*ringpop.Ringpop, size),
		channels:     make([]*tchannel.Channel, size),
		seedNode:     seed,
	}

	for i := 0; i < size; i++ {
		var err error
		cluster.channels[i], err = tchannel.NewChannel(name, nil)
		if err != nil {
			log.WithField(TagErr, err).Error("Failed to create tchannel")
			return nil
		}
		listenAddr := ipAddr + ":0"
		err = cluster.channels[i].ListenAndServe(listenAddr)
		if err != nil {
			log.WithField(TagErr, err).Error("tchannel listen failed")
			return nil
		}
		cluster.hostUUIDs[i] = uuid.New()
		cluster.hostAddrs[i] = cluster.channels[i].PeerInfo().HostPort
		cluster.hostInfoList[i] = HostInfo{addr: cluster.hostAddrs[i]}
		cluster.rings[i], err = ringpop.New(name, ringpop.Channel(cluster.channels[i]))
		if err != nil {
			log.WithField(TagErr, err).Error("failed to create ringpop instance")
			return nil
		}
	}

	// if seed node is already supplied, use it; if not, set it
	if cluster.seedNode == "" {
		cluster.seedNode = cluster.hostAddrs[0]
	}
	log.Infof("seedNode: %v", cluster.seedNode)
	bOptions := new(swim.BootstrapOptions)
	bOptions.DiscoverProvider = statichosts.New(cluster.seedNode) // just use the first addr as the seed
	bOptions.MaxJoinDuration = time.Duration(time.Second * 10)
	bOptions.JoinSize = 1

	for i := 0; i < size; i++ {
		_, err := cluster.rings[i].Bootstrap(bOptions)
		if err != nil {
			log.WithField(TagErr, err).Error("failed to bootstrap ringpop")
			return nil
		}
		labels, err := cluster.rings[i].Labels()
		if err != nil {
			log.Errorf("labels failed: %v", err)
		} else {
			labels.Set("serviceName", sName)
		}
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
		if 0 == strings.Compare(c.hostUUIDs[i], uuid) {
			c.rings[i].Destroy()
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
			c.rings[i].Destroy()
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
		if strings.Compare(hi.addr, addr) == 0 {
			return hi, true
		}
	}
	return HostInfo{}, false
}
