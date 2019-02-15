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

package config

import (
	"encoding/json"
	"github.com/uber/cadence/common/blobstore/filestore"
	"time"

	"github.com/uber-go/tally/m3"
	"github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/ringpop-go/discovery"
)

type (
	// Config contains the configuration for a set of cadence services
	Config struct {
		// Ringpop is the ringpop related configuration
		Ringpop Ringpop `yaml:"ringpop"`
		// Persistence contains the configuration for cadence datastores
		Persistence Persistence `yaml:"persistence"`
		// Log is the logging config
		Log Logger `yaml:"log"`
		// ClustersInfo is the config containing all valid clusters and active cluster
		ClustersInfo ClustersInfo `yaml:"clustersInfo"`
		// DCRedirectionPolicy contains the frontend datacenter redirection policy
		DCRedirectionPolicy DCRedirectionPolicy `yaml:"dcRedirectionPolicy"`
		// Services is a map of service name to service config items
		Services map[string]Service `yaml:"services"`
		// Kafka is the config for connecting to kafka
		Kafka messaging.KafkaConfig `yaml:"kafka"`
		// Archival is the config for archival
		Archival Archival `yaml:"archival"`
		// ElasticSearch if config for connecting to ElasticSearch
		ElasticSearch elasticsearch.Config `yaml:elasticsearch`
	}

	// Service contains the service specific config items
	Service struct {
		// TChannel is the tchannel configuration
		RPC RPC `yaml:"rpc"`
		// Metrics is the metrics subsystem configuration
		Metrics Metrics `yaml:"metrics"`
		// PProf is the PProf configuration
		PProf PProf `yaml:"pprof"`
	}

	// PProf contains the rpc config items
	PProf struct {
		// Port is the port on which the PProf will bind to
		Port int `yaml:"port"`
	}

	// RPC contains the rpc config items
	RPC struct {
		// Port is the port  on which the channel will bind to
		Port int `yaml:"port"`
		// BindOnLocalHost is true if localhost is the bind address
		BindOnLocalHost bool `yaml:"bindOnLocalHost"`
		// BindOnIP can be used to bind service on specific ip (eg. `0.0.0.0`) -
		// check net.ParseIP for supported syntax, only IPv4 is supported,
		// mutually exclusive with `BindOnLocalHost` option
		BindOnIP string `yaml:"bindOnIP"`
		// DisableLogging disables all logging for rpc
		DisableLogging bool `yaml:"disableLogging"`
		// LogLevel is the desired log level
		LogLevel string `yaml:"logLevel"`
	}

	// Ringpop contains the ringpop config items
	Ringpop struct {
		// Name to be used in ringpop advertisement
		Name string `yaml:"name" validate:"nonzero"`
		// BootstrapMode is a enum that defines the ringpop bootstrap method
		BootstrapMode BootstrapMode `yaml:"bootstrapMode"`
		// BootstrapHosts is a list of seed hosts to be used for ringpop bootstrap
		BootstrapHosts []string `yaml:"bootstrapHosts"`
		// BootstrapFile is the file path to be used for ringpop bootstrap
		BootstrapFile string `yaml:"bootstrapFile"`
		// MaxJoinDuration is the max wait time to join the ring
		MaxJoinDuration time.Duration `yaml:"maxJoinDuration"`
		// Custom discovery provider, cannot be specified through yaml
		DiscoveryProvider discovery.DiscoverProvider `yaml:"-"`
	}

	// Persistence contains the configuration for data store / persistence layer
	Persistence struct {
		// DefaultStore is the name of the default data store to use
		DefaultStore string `yaml:"defaultStore" validate:"nonzero"`
		// VisibilityStore is the name of the datastore to be used for visibility records
		VisibilityStore string `yaml:"visibilityStore" validate:"nonzero"`
		// HistoryMaxConns is the desired number of conns to history store. Value specified
		// here overrides the MaxConns config specified as part of datastore
		HistoryMaxConns int `yaml:"historyMaxConns"`
		// NumHistoryShards is the desired number of history shards. This config doesn't
		// belong here, needs refactoring
		NumHistoryShards int `yaml:"numHistoryShards" validate:"nonzero"`
		// DataStores contains the configuration for all datastores
		DataStores map[string]DataStore `yaml:"datastores"`
		// SamplingConfig is config for visibility sampling
		SamplingConfig SamplingConfig
	}

	// DataStore is the configuration for a single datastore
	DataStore struct {
		// Cassandra contains the config for a cassandra datastore
		Cassandra *Cassandra `yaml:"cassandra"`
		// SQL contains the config for a SQL based datastore
		SQL *SQL `yaml:"sql"`
	}

	// SamplingConfig is config for visibility sampling
	SamplingConfig struct {
		// VisibilityOpenMaxQPS max QPS for record open workflows
		VisibilityOpenMaxQPS dynamicconfig.IntPropertyFnWithDomainFilter
		// VisibilityClosedMaxQPS max QPS for record closed workflows
		VisibilityClosedMaxQPS dynamicconfig.IntPropertyFnWithDomainFilter
		// VisibilityListMaxQPS max QPS for list workflow
		VisibilityListMaxQPS dynamicconfig.IntPropertyFnWithDomainFilter
	}

	// Cassandra contains configuration to connect to Cassandra cluster
	Cassandra struct {
		// Hosts is a csv of cassandra endpoints
		Hosts string `yaml:"hosts" validate:"nonzero"`
		// Port is the cassandra port used for connection by gocql client
		Port int `yaml:"port"`
		// User is the cassandra user used for authentication by gocql client
		User string `yaml:"user"`
		// Password is the cassandra password used for authentication by gocql client
		Password string `yaml:"password"`
		// keyspace is the cassandra keyspace
		Keyspace string `yaml:"keyspace" validate:"nonzero"`
		// Consistency is the default cassandra consistency level
		Consistency string `yaml:"consistency"`
		// Datacenter is the data center filter arg for cassandra
		Datacenter string `yaml:"datacenter"`
		// MaxQPS is the max request rate to this datastore
		MaxQPS int `yaml:"maxQPS"`
		// MaxConns is the max number of connections to this datastore for a single keyspace
		MaxConns int `yaml:"maxConns"`
	}

	// SQL is the configuration for connecting to a SQL backed datastore
	SQL struct {
		// User is the username to be used for the conn
		User string `yaml:"user"`
		// Password is the password corresponding to the user name
		Password string `yaml:"password"`
		// DriverName is the name of SQL driver
		DriverName string `yaml:"driverName" validate:"nonzero"`
		// DatabaseName is the name of SQL database to connect to
		DatabaseName string `yaml:"databaseName" validate:"nonzero"`
		// ConnectAddr is the remote addr of the database
		ConnectAddr string `yaml:"connectAddr" validate:"nonzero"`
		// ConnectProtocol is the protocol that goes with the ConnectAddr ex - tcp, unix
		ConnectProtocol string `yaml:"connectProtocol" validate:"nonzero"`
		// MaxQPS the max request rate on this datastore
		MaxQPS int `yaml:"maxQPS"`
		// MaxConns the max number of connections to this datastore
		MaxConns int `yaml:"maxConns"`
	}

	// Replicator describes the configuration of replicator
	Replicator struct{}

	// Logger contains the config items for logger
	Logger struct {
		// Stdout is true if the output needs to goto standard out
		Stdout bool `yaml:"stdout"`
		// Level is the desired log level
		Level string `yaml:"level"`
		// OutputFile is the path to the log output file
		OutputFile string `yaml:"outputFile"`
	}

	// ClustersInfo contains the all cluster names and active cluster
	ClustersInfo struct {
		// EnableGlobalDomain whether the global domain is enabled, this attr should be discarded when
		// cross DC is made public
		EnableGlobalDomain bool `yaml:"enableGlobalDomain"`
		// FailoverVersionIncrement is the increment of each cluster failover version
		FailoverVersionIncrement int64 `yaml:"failoverVersionIncrement"`
		// MasterClusterName is the master cluster name, only the master cluster can register / update domain
		// all clusters can do domain failover
		MasterClusterName string `yaml:"masterClusterName"`
		// CurrentClusterName is the name of the current cluster
		CurrentClusterName string `yaml:"currentClusterName"`
		// ClusterInitialFailoverVersions contains all cluster names to corresponding initial failover version
		ClusterInitialFailoverVersions map[string]int64 `yaml:"clusterInitialFailoverVersion"`
		// ClusterAddress contains all cluster names to corresponding address
		ClusterAddress map[string]Address `yaml:"clusterAddress"`
	}

	// Address indicate the remote cluster's service name and address
	Address struct {
		// RPCName indicate the remote service name
		RPCName string `yaml:"rpcName"`
		// Address indicate the remote service IP address
		RPCAddress string `yaml:"rpcAddress"`
	}

	// DCRedirectionPolicy contains the frontend datacenter redirection policy
	DCRedirectionPolicy struct {
		Policy string `yaml:"policy"`
		ToDC   string `yaml:"toDC"`
	}

	// Metrics contains the config items for metrics subsystem
	Metrics struct {
		// M3 is the configuration for m3 metrics reporter
		M3 *m3.Configuration `yaml:"m3"`
		// Statsd is the configuration for statsd reporter
		Statsd *Statsd `yaml:"statsd"`
		// Tags is the set of key-value pairs to be reported
		// as part of every metric
		Tags map[string]string `yaml:"tags"`
	}

	// Statsd contains the config items for statsd metrics reporter
	Statsd struct {
		// The host and port of the statsd server
		HostPort string `yaml:"hostPort" validate:"nonzero"`
		// The prefix to use in reporting to statsd
		Prefix string `yaml:"prefix" validate:"nonzero"`
		// FlushInterval is the maximum interval for sending packets.
		// If it is not specified, it defaults to 1 second.
		FlushInterval time.Duration `yaml:"flushInterval"`
		// FlushBytes specifies the maximum udp packet size you wish to send.
		// If FlushBytes is unspecified, it defaults  to 1432 bytes, which is
		// considered safe for local traffic.
		FlushBytes int `yaml:"flushBytes"`
	}

	// Archival contains the config for archival
	Archival struct {
		// Enabled whether archival is enabled
		Enabled bool `yaml:"enabled"`
		// Filestore the configuration for file based blobstore
		Filestore filestore.Config `yaml:"filestore"`
	}

	// BootstrapMode is an enum type for ringpop bootstrap mode
	BootstrapMode int
)

// Validate validates this config
func (c *Config) Validate() error {
	return c.Persistence.Validate()
}

// String converts the config object into a string
func (c *Config) String() string {
	out, _ := json.MarshalIndent(c, "", "    ")
	return string(out)
}
