package cluster

import (
	"github.com/google/uuid"
)

const (
	// TestCurrentClusterInitialFailoverVersion is initial failover version for current cluster
	TestCurrentClusterInitialFailoverVersion = int64(1)
	// TestAlternativeClusterInitialFailoverVersion is initial failover version for alternative cluster
	TestAlternativeClusterInitialFailoverVersion = int64(2)
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
	// TestCurrentClusterFrontendHTTPAddress is the ip port HTTP address of current cluster. Currently, gRPC port+10. See tests/onebox.go:FrontendHTTPAddress
	TestCurrentClusterFrontendHTTPAddress = "127.0.0.1:7144"
	// TestAlternativeClusterFrontendHTTPAddress is the ip port HTTP address of the alternative cluster. Currently, gRPC port+10. See tests/onebox.go:FrontendHTTPAddress
	TestAlternativeClusterFrontendHTTPAddress = "127.0.0.1:8144"
)

var (
	// TestAllClusterNames is the all cluster names used for test
	TestAllClusterNames = []string{TestCurrentClusterName, TestAlternativeClusterName}
	// TestAllClusterInfo is the same as above, just convenient for test mocking
	TestAllClusterInfo = map[string]ClusterInformation{
		TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             TestCurrentClusterFrontendAddress,
			HTTPAddress:            TestCurrentClusterFrontendHTTPAddress,
			ShardCount:             8,
			ClusterID:              uuid.NewString(),
		},
		TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             TestAlternativeClusterFrontendAddress,
			HTTPAddress:            TestAlternativeClusterFrontendHTTPAddress,
			ShardCount:             4,
			ClusterID:              uuid.NewString(),
		},
	}

	// TestSingleDCAllClusterNames is the all cluster names used for test
	TestSingleDCAllClusterNames = []string{TestCurrentClusterName}
	// TestSingleDCClusterInfo is the same as above, just convenient for test mocking
	TestSingleDCClusterInfo = map[string]ClusterInformation{
		TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             TestCurrentClusterFrontendAddress,
			HTTPAddress:            TestCurrentClusterFrontendHTTPAddress,
			ClusterID:              uuid.NewString(),
		},
	}
)

// NewTestClusterMetadataConfig return an cluster metadata config
func NewTestClusterMetadataConfig(enableGlobalNamespace bool, isMasterCluster bool) *Config {
	masterClusterName := TestCurrentClusterName
	if !isMasterCluster {
		masterClusterName = TestAlternativeClusterName
	}

	if enableGlobalNamespace {
		return &Config{
			EnableGlobalNamespace:    true,
			FailoverVersionIncrement: TestFailoverVersionIncrement,
			MasterClusterName:        masterClusterName,
			CurrentClusterName:       TestCurrentClusterName,
			ClusterInformation:       TestAllClusterInfo,
		}
	}

	return &Config{
		EnableGlobalNamespace:    false,
		FailoverVersionIncrement: TestFailoverVersionIncrement,
		MasterClusterName:        TestCurrentClusterName,
		CurrentClusterName:       TestCurrentClusterName,
		ClusterInformation:       TestSingleDCClusterInfo,
	}
}
