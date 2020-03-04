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
	"time"

	"github.com/uber/cadence/common/auth"

	"github.com/uber-go/tally/m3"
	"github.com/uber-go/tally/prometheus"
	"github.com/uber/ringpop-go/discovery"

	"github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

const (
	// ReplicationConsumerTypeKafka means consuming replication tasks from kafka.
	ReplicationConsumerTypeKafka = "kafka"
	// ReplicationConsumerTypeRPC means pulling source DC for replication tasks.
	ReplicationConsumerTypeRPC = "rpc"
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
		// ClusterMetadata is the config containing all valid clusters and active cluster
		ClusterMetadata *ClusterMetadata `yaml:"clusterMetadata"`
		// DCRedirectionPolicy contains the frontend datacenter redirection policy
		DCRedirectionPolicy DCRedirectionPolicy `yaml:"dcRedirectionPolicy"`
		// Services is a map of service name to service config items
		Services map[string]Service `yaml:"services"`
		// Kafka is the config for connecting to kafka
		Kafka messaging.KafkaConfig `yaml:"kafka"`
		// Archival is the config for archival
		Archival Archival `yaml:"archival"`
		// PublicClient is config for connecting to cadence frontend
		PublicClient PublicClient `yaml:"publicClient"`
		// DynamicConfigClient is the config for setting up the file based dynamic config client
		// Filepath should be relative to the root directory
		DynamicConfigClient dynamicconfig.FileBasedClientConfig `yaml:"dynamicConfigClient"`
		// DomainDefaults is the default config for every domain
		DomainDefaults DomainDefaults `yaml:"domainDefaults"`
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
		// AdvancedVisibilityStore is the name of the datastore to be used for visibility records
		AdvancedVisibilityStore string `yaml:"advancedVisibilityStore"`
		// HistoryMaxConns is the desired number of conns to history store. Value specified
		// here overrides the MaxConns config specified as part of datastore
		HistoryMaxConns int `yaml:"historyMaxConns"`
		// NumHistoryShards is the desired number of history shards. This config doesn't
		// belong here, needs refactoring
		NumHistoryShards int `yaml:"numHistoryShards" validate:"nonzero"`
		// DataStores contains the configuration for all datastores
		DataStores map[string]DataStore `yaml:"datastores"`
		// VisibilityConfig is config for visibility sampling
		VisibilityConfig *VisibilityConfig `yaml:"-" json:"-"`
		// TransactionSizeLimit is the largest allowed transaction size
		TransactionSizeLimit dynamicconfig.IntPropertyFn `yaml:"-" json:"-"`
	}

	// DataStore is the configuration for a single datastore
	DataStore struct {
		// Cassandra contains the config for a cassandra datastore
		Cassandra *Cassandra `yaml:"cassandra"`
		// SQL contains the config for a SQL based datastore
		SQL *SQL `yaml:"sql"`
		// Custom contains the config for custom datastore implementation
		CustomDataStoreConfig *CustomDatastoreConfig `yaml:"customDatastore"`
		// ElasticSearch contains the config for a ElasticSearch datastore
		ElasticSearch *elasticsearch.Config `yaml:"elasticsearch"`
	}

	// VisibilityConfig is config for visibility sampling
	VisibilityConfig struct {
		// EnableSampling for visibility
		EnableSampling dynamicconfig.BoolPropertyFn `yaml:"-" json:"-"`
		// EnableReadFromClosedExecutionV2 read closed from v2 table
		EnableReadFromClosedExecutionV2 dynamicconfig.BoolPropertyFn `yaml:"-" json:"-"`
		// VisibilityOpenMaxQPS max QPS for record open workflows
		VisibilityOpenMaxQPS dynamicconfig.IntPropertyFnWithDomainFilter `yaml:"-" json:"-"`
		// VisibilityClosedMaxQPS max QPS for record closed workflows
		VisibilityClosedMaxQPS dynamicconfig.IntPropertyFnWithDomainFilter `yaml:"-" json:"-"`
		// VisibilityListMaxQPS max QPS for list workflow
		VisibilityListMaxQPS dynamicconfig.IntPropertyFnWithDomainFilter `yaml:"-" json:"-"`
		// ESIndexMaxResultWindow ElasticSearch index setting max_result_window
		ESIndexMaxResultWindow dynamicconfig.IntPropertyFn `yaml:"-" json:"-"`
		// MaxQPS is overall max QPS
		MaxQPS dynamicconfig.IntPropertyFn `yaml:"-" json:"-"`
		// ValidSearchAttributes is legal indexed keys that can be used in list APIs
		ValidSearchAttributes dynamicconfig.MapPropertyFn `yaml:"-" json:"-"`
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
		// Datacenter is the data center filter arg for cassandra
		Datacenter string `yaml:"datacenter"`
		// MaxConns is the max number of connections to this datastore for a single keyspace
		MaxConns int `yaml:"maxConns"`
		// TLS configuration
		TLS *auth.TLS `yaml:"tls"`
	}

	// SQL is the configuration for connecting to a SQL backed datastore
	SQL struct {
		// User is the username to be used for the conn
		User string `yaml:"user"`
		// Password is the password corresponding to the user name
		Password string `yaml:"password"`
		// PluginName is the name of SQL plugin
		PluginName string `yaml:"pluginName" validate:"nonzero"`
		// DatabaseName is the name of SQL database to connect to
		DatabaseName string `yaml:"databaseName" validate:"nonzero"`
		// ConnectAddr is the remote addr of the database
		ConnectAddr string `yaml:"connectAddr" validate:"nonzero"`
		// ConnectProtocol is the protocol that goes with the ConnectAddr ex - tcp, unix
		ConnectProtocol string `yaml:"connectProtocol" validate:"nonzero"`
		// ConnectAttributes is a set of key-value attributes to be sent as part of connect data_source_name url
		ConnectAttributes map[string]string `yaml:"connectAttributes"`
		// MaxConns the max number of connections to this datastore
		MaxConns int `yaml:"maxConns"`
		// MaxIdleConns is the max number of idle connections to this datastore
		MaxIdleConns int `yaml:"maxIdleConns"`
		// MaxConnLifetime is the maximum time a connection can be alive
		MaxConnLifetime time.Duration `yaml:"maxConnLifetime"`
		// NumShards is the number of storage shards to use for tables
		// in a sharded sql database. The default value for this param is 1
		NumShards int `yaml:"nShards"`
		// TLS is the configuration for TLS connections
		TLS *auth.TLS `yaml:"tls"`
	}

	// CustomDatastoreConfig is the configuration for connecting to a custom datastore that is not supported by cadence core
	CustomDatastoreConfig struct {
		// Name of the custom datastore
		Name string `yaml:"name"`
		// ConnectAttributes is a set of key-value attributes that can be used by AbstractDatastoreFactory implementation
		ConnectAttributes map[string]string `yaml:"connectAttributes"`
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

	// ClusterMetadata contains the all cluster which participated in cross DC
	ClusterMetadata struct {
		EnableGlobalDomain bool `yaml:"enableGlobalDomain"`
		// ReplicationConsumerConfig determines how we consume replication tasks.
		ReplicationConsumer *ReplicationConsumerConfig `yaml:"replicationConsumer"`
		// FailoverVersionIncrement is the increment of each cluster version when failover happens
		FailoverVersionIncrement int64 `yaml:"failoverVersionIncrement"`
		// MasterClusterName is the master cluster name, only the master cluster can register / update domain
		// all clusters can do domain failover
		MasterClusterName string `yaml:"masterClusterName"`
		// CurrentClusterName is the name of the current cluster
		CurrentClusterName string `yaml:"currentClusterName"`
		// ClusterInformation contains all cluster names to corresponding information about that cluster
		ClusterInformation map[string]ClusterInformation `yaml:"clusterInformation"`
	}

	// ClusterInformation contains the information about each cluster which participated in cross DC
	ClusterInformation struct {
		Enabled                bool  `yaml:"enabled"`
		InitialFailoverVersion int64 `yaml:"initialFailoverVersion"`
		// RPCName indicate the remote service name
		RPCName string `yaml:"rpcName"`
		// Address indicate the remote service address(Host:Port). Host can be DNS name.
		RPCAddress string `yaml:"rpcAddress"`
	}

	// ReplicationConsumerConfig contains config for replication consumer
	ReplicationConsumerConfig struct {
		// Type determines how we consume replication tasks. It can be either kafka(default) or rpc.
		Type string `yaml:"type"`
	}

	// ReplicationTaskProcessorConfig is the config for replication task processor.
	ReplicationTaskProcessorConfig struct {
		NoTaskInitialWaitIntervalSecs int     `yaml:"noTaskInitialWaitIntervalSecs"`
		NoTaskWaitBackoffCoefficient  float64 `yaml:"noTaskWaitBackoffCoefficient"`
		NoTaskMaxWaitIntervalSecs     int     `yaml:"noTaskMaxWaitIntervalSecs"`
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
		// Prometheus is the configuration for prometheus reporter
		Prometheus *prometheus.Configuration `yaml:"prometheus"`
		// Tags is the set of key-value pairs to be reported
		// as part of every metric
		Tags map[string]string `yaml:"tags"`
		// Prefix sets the prefix to all outgoing metrics
		Prefix string `yaml:"prefix"`
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
		// History is the config for the history archival
		History HistoryArchival `yaml:"history"`
		// Visibility is the config for visibility archival
		Visibility VisibilityArchival `yaml:"visibility"`
	}

	// HistoryArchival contains the config for history archival
	HistoryArchival struct {
		// Status is the status of history archival either: enabled, disabled, or paused
		Status string `yaml:"status"`
		// EnableRead whether history can be read from archival
		EnableRead bool `yaml:"enableRead"`
		// Provider contains the config for all history archivers
		Provider *HistoryArchiverProvider `yaml:"provider"`
	}

	// HistoryArchiverProvider contains the config for all history archivers
	HistoryArchiverProvider struct {
		Filestore *FilestoreArchiver `yaml:"filestore"`
		Gstorage  *GstorageArchiver  `yaml:"gstorage"`
		S3store   *S3Archiver        `yaml:"s3store"`
	}

	// VisibilityArchival contains the config for visibility archival
	VisibilityArchival struct {
		// Status is the status of visibility archival either: enabled, disabled, or paused
		Status string `yaml:"status"`
		// EnableRead whether visibility can be read from archival
		EnableRead bool `yaml:"enableRead"`
		// Provider contains the config for all visibility archivers
		Provider *VisibilityArchiverProvider `yaml:"provider"`
	}

	// VisibilityArchiverProvider contains the config for all visibility archivers
	VisibilityArchiverProvider struct {
		Filestore *FilestoreArchiver `yaml:"filestore"`
		S3store   *S3Archiver        `yaml:"s3store"`
		Gstorage  *GstorageArchiver  `yaml:"gstorage"`
	}

	// FilestoreArchiver contain the config for filestore archiver
	FilestoreArchiver struct {
		FileMode string `yaml:"fileMode"`
		DirMode  string `yaml:"dirMode"`
	}

	// GstorageArchiver contain the config for google storage archiver
	GstorageArchiver struct {
		CredentialsPath string `yaml:"credentialsPath"`
	}

	// S3Archiver contains the config for S3 archiver
	S3Archiver struct {
		Region           string  `yaml:"region"`
		Endpoint         *string `yaml:"endpoint"`
		S3ForcePathStyle bool    `yaml:"s3ForcePathStyle"`
	}

	// PublicClient is config for connecting to cadence frontend
	PublicClient struct {
		// HostPort is the host port to connect on. Host can be DNS name
		HostPort string `yaml:"hostPort" validate:"nonzero"`
		// interval to refresh DNS. Default to 10s
		RefreshInterval time.Duration `yaml:"RefreshInterval"`
	}

	// DomainDefaults is the default config for each domain
	DomainDefaults struct {
		// Archival is the default archival config for each domain
		Archival ArchivalDomainDefaults `yaml:"archival"`
	}

	// ArchivalDomainDefaults is the default archival config for each domain
	ArchivalDomainDefaults struct {
		// History is the domain default history archival config for each domain
		History HistoryArchivalDomainDefaults `yaml:"history"`
		// Visibility is the domain default visibility archival config for each domain
		Visibility VisibilityArchivalDomainDefaults `yaml:"visibility"`
	}

	// HistoryArchivalDomainDefaults is the default history archival config for each domain
	HistoryArchivalDomainDefaults struct {
		// Status is the domain default status of history archival: enabled or disabled
		Status string `yaml:"status"`
		// URI is the domain default URI for history archiver
		URI string `yaml:"URI"`
	}

	// VisibilityArchivalDomainDefaults is the default visibility archival config for each domain
	VisibilityArchivalDomainDefaults struct {
		// Status is the domain default status of visibility archival: enabled or disabled
		Status string `yaml:"status"`
		// URI is the domain default URI for visibility archiver
		URI string `yaml:"URI"`
	}

	// BootstrapMode is an enum type for ringpop bootstrap mode
	BootstrapMode int
)

// Validate validates this config
func (c *Config) Validate() error {
	if err := c.Persistence.Validate(); err != nil {
		return err
	}
	return c.Archival.Validate(&c.DomainDefaults.Archival)
}

// String converts the config object into a string
func (c *Config) String() string {
	out, _ := json.MarshalIndent(c, "", "    ")
	return string(out)
}
