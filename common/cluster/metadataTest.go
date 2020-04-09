package cluster

import (
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

const (
	// TestCurrentClusterInitialFailoverVersion is initial failover version for current cluster
	TestCurrentClusterInitialFailoverVersion = int64(0)
	// TestAlternativeClusterInitialFailoverVersion is initial failover version for alternative cluster
	TestAlternativeClusterInitialFailoverVersion = int64(1)
	// TestFailoverVersionIncrement is failover version increment used for test
	TestFailoverVersionIncrement = int64(10)
	// TestCurrentClusterName is current cluster used for test
	TestCurrentClusterName = "active"
	// TestAlternativeClusterName is alternative cluster used for test
	TestAlternativeClusterName = "standby"
	// TestCurrentClusterFrontendAddress is the ip port address of current cluster
	TestCurrentClusterFrontendAddress = "127.0.0.1:7134"
	// TestAlternativeClusterFrontendAddress is the ip port address of alternative cluster
	TestAlternativeClusterFrontendAddress = "127.0.0.1:8134"
)

var (
	// TestAllClusterNames is the all cluster names used for test
	TestAllClusterNames = []string{TestCurrentClusterName, TestAlternativeClusterName}
	// TestAllClusterInfo is the same as above, just convenient for test mocking
	TestAllClusterInfo = map[string]config.ClusterInformation{
		TestCurrentClusterName: config.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCName:                common.FrontendServiceName,
			RPCAddress:             TestCurrentClusterFrontendAddress,
		},
		TestAlternativeClusterName: config.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: TestAlternativeClusterInitialFailoverVersion,
			RPCName:                common.FrontendServiceName,
			RPCAddress:             TestAlternativeClusterFrontendAddress,
		},
	}

	// TestSingleDCAllClusterNames is the all cluster names used for test
	TestSingleDCAllClusterNames = []string{TestCurrentClusterName}
	// TestSingleDCClusterInfo is the same as above, just convenient for test mocking
	TestSingleDCClusterInfo = map[string]config.ClusterInformation{
		TestCurrentClusterName: config.ClusterInformation{
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCName:                common.FrontendServiceName,
			RPCAddress:             TestCurrentClusterFrontendAddress,
		},
	}
)

// GetTestClusterMetadata return an cluster metadata instance, which is initialized
func GetTestClusterMetadata(enableGlobalNamespace bool, isMasterCluster bool) Metadata {
	masterClusterName := TestCurrentClusterName
	if !isMasterCluster {
		masterClusterName = TestAlternativeClusterName
	}

	if enableGlobalNamespace {
		return NewMetadata(
			loggerimpl.NewNopLogger(),
			dynamicconfig.GetBoolPropertyFn(true),
			TestFailoverVersionIncrement,
			masterClusterName,
			TestCurrentClusterName,
			TestAllClusterInfo,
			&config.ReplicationConsumerConfig{
				Type: config.ReplicationConsumerTypeRPC,
			},
		)
	}

	return NewMetadata(
		loggerimpl.NewNopLogger(),
		dynamicconfig.GetBoolPropertyFn(false),
		TestFailoverVersionIncrement,
		TestCurrentClusterName,
		TestCurrentClusterName,
		TestSingleDCClusterInfo,
		nil,
	)
}
