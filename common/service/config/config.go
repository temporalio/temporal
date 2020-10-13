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

package config

import (
	"encoding/json"
	"time"

	"github.com/uber-go/tally/m3"
	"github.com/uber-go/tally/prometheus"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/service/dynamicconfig"
)

const (
	// ReplicationConsumerTypeKafka means consuming replication tasks from kafka.
	ReplicationConsumerTypeKafka = "kafka"
	// ReplicationConsumerTypeRPC means pulling source DC for replication tasks.
	ReplicationConsumerTypeRPC = "rpc"
)

type (
	// Config contains the configuration for a set of temporal services
	Config struct {
		// Global is process-wide service-related configuration
		Global Global `yaml:"global"`
		// Persistence contains the configuration for temporal datastores
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
		// PublicClient is config for connecting to temporal frontend
		PublicClient PublicClient `yaml:"publicClient"`
		// DynamicConfigClient is the config for setting up the file based dynamic config client
		// Filepath should be relative to the root directory
		DynamicConfigClient dynamicconfig.FileBasedClientConfig `yaml:"dynamicConfigClient"`
		// NamespaceDefaults is the default config for every namespace
		NamespaceDefaults NamespaceDefaults `yaml:"namespaceDefaults"`
	}

	// Service contains the service specific config items
	Service struct {
		// RPC is the rpc configuration
		RPC RPC `yaml:"rpc"`
		// Metrics is the metrics subsystem configuration
		Metrics Metrics `yaml:"metrics"`
	}

	// PProf contains the config items for the pprof utility
	PProf struct {
		// Port is the port on which the PProf will bind to
		Port int `yaml:"port"`
	}

	// RPC contains the rpc config items
	RPC struct {
		// GRPCPort is the port  on which gRPC will listen
		GRPCPort int `yaml:"grpcPort"`
		// Port used for membership listener
		MembershipPort int `yaml:"membershipPort"`
		// BindOnLocalHost is true if localhost is the bind address
		BindOnLocalHost bool `yaml:"bindOnLocalHost"`
		// BindOnIP can be used to bind service on specific ip (eg. `0.0.0.0`) -
		// check net.ParseIP for supported syntax, only IPv4 is supported,
		// mutually exclusive with `BindOnLocalHost` option
		BindOnIP string `yaml:"bindOnIP"`
	}

	// Global contains config items that apply process-wide to all services
	Global struct {
		// Membership is the ringpop related configuration
		Membership Membership `yaml:"membership"`
		// PProf is the PProf configuration
		PProf PProf `yaml:"pprof"`
		// TLS controls the communication encryption configuration
		TLS RootTLS `yaml:"tls"`
	}

	// RootTLS contains all TLS settings for the Temporal server
	RootTLS struct {
		// Internode controls backend service communication TLS settings.
		Internode GroupTLS `yaml:"internode"`
		// Frontend controls SDK Client to Frontend communication TLS settings.
		Frontend GroupTLS `yaml:"frontend"`
	}

	// GroupTLS contains an instance client and server TLS settings
	GroupTLS struct {
		// Client handles client TLS settings
		Client ClientTLS `yaml:"client"`
		// Server handles the server (listener) TLS settings
		Server ServerTLS `yaml:"server"`
	}

	// ServerTLS contains items to load server TLS configuration
	ServerTLS struct {
		// The path to the file containing the PEM-encoded public key of the certificate to use.
		CertFile string `yaml:"certFile"`
		// The path to the file containing the PEM-encoded private key of the certificate to use.
		KeyFile string `yaml:"keyFile"`
		// A list of paths to files containing the PEM-encoded public key of the Certificate Authorities you wish to trust for client authentication.
		// This value is ignored if `requireClientAuth` is not enabled.
		ClientCAFiles []string `yaml:"clientCaFiles"`
		// Requires clients to authenticate with a certificate when connecting, otherwise known as mutual TLS.
		RequireClientAuth bool `yaml:"requireClientAuth"`
	}

	// ClientTLS contains TLS configuration for clients.
	ClientTLS struct {
		// DNS name to validate against for server to server connections.
		// Required when TLS is enabled in a multi-host cluster.
		// This name should be referenced by the certificate specified in the ServerTLS section.
		ServerName string `yaml:"serverName"`

		// Optional - A list of paths to files containing the PEM-encoded public key of the Certificate Authorities you wish to trust.
		RootCAFiles []string `yaml:"rootCaFiles"`
	}

	// Membership contains config items related to the membership layer of temporal
	Membership struct {
		// MaxJoinDuration is the max wait time to join the gossip ring
		MaxJoinDuration time.Duration `yaml:"maxJoinDuration"`
		// BroadcastAddress is used as the address that is communicated to remote nodes to connect on.
		// This is generally used when BindOnIP would be the same across several nodes (ie: 0.0.0.0)
		// and for nat traversal scenarios. Check net.ParseIP for supported syntax, only IPv4 is supported.
		BroadcastAddress string `yaml:"broadcastAddress"`
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
		NumHistoryShards int32 `yaml:"numHistoryShards" validate:"nonzero"`
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
		// VisibilityOpenMaxQPS max QPS for record open workflows
		VisibilityOpenMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter `yaml:"-" json:"-"`
		// VisibilityClosedMaxQPS max QPS for record closed workflows
		VisibilityClosedMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter `yaml:"-" json:"-"`
		// VisibilityListMaxQPS max QPS for list workflow
		VisibilityListMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter `yaml:"-" json:"-"`
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
		// ConnectTimeout is a timeout for initial dial to cassandra server (default: 600 milliseconds)
		ConnectTimeout time.Duration `yaml:"connectTimeout"`
		// TLS configuration
		TLS *auth.TLS `yaml:"tls"`
		// Consistency configuration (defaults to LOCAL_QUORUM / LOCAL_SERIAL for all stores if this field not set)
		Consistency *CassandraStoreConsistency `yaml:"consistency"`
	}

	// CassandraStoreConsistency enables you to set the consistency settings for each Cassandra Persistence Store for Temporal
	CassandraStoreConsistency struct {
		// Default defines the consistency level for ALL stores.
		// Defaults to LOCAL_QUORUM and LOCAL_SERIAL if not set
		Default *CassandraConsistencySettings `yaml:"default"`
	}

	// CassandraConsistencySettings sets the default consistency level for regular & serial queries to Cassandra.
	CassandraConsistencySettings struct {
		// Consistency sets the default consistency level. Values identical to gocql Consistency values. (defaults to LOCAL_QUORUM if not set).
		Consistency string `yaml:"consistency"`
		// SerialConsistency sets the consistency for the serial prtion of queries. Values identical to gocql SerialConsistency values. (defaults to LOCAL_SERIAL if not set)
		SerialConsistency string `yaml:"serialConsistency"`
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
		// EXPERIMENTAL - TaskScanPartitions is the number of partitions to sequentially scan during ListTaskQueue operations.
		// This is used for in a sharded sql database such as Vitess for heavy task workloads to minimize scatter gather.
		// The default value for this param is 1, and should not be configured without a thorough understanding of what this does.
		TaskScanPartitions int `yaml:"taskScanPartitions"`
		// TLS is the configuration for TLS connections
		TLS *auth.TLS `yaml:"tls"`
	}

	// CustomDatastoreConfig is the configuration for connecting to a custom datastore that is not supported by temporal core
	CustomDatastoreConfig struct {
		// Name of the custom datastore
		Name string `yaml:"name"`
		// Options is a set of key-value attributes that can be used by AbstractDatastoreFactory implementation
		Options map[string]string `yaml:"options"`
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
		EnableGlobalNamespace bool `yaml:"enableGlobalNamespace"`
		// ReplicationConsumerConfig determines how we consume replication tasks.
		ReplicationConsumer *ReplicationConsumerConfig `yaml:"replicationConsumer"`
		// FailoverVersionIncrement is the increment of each cluster version when failover happens
		FailoverVersionIncrement int64 `yaml:"failoverVersionIncrement"`
		// MasterClusterName is the master cluster name, only the master cluster can register / update namespace
		// all clusters can do namespace failover
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
		// TODO: remove RPCName (not used with gRPC)
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
		// State is the state of history archival either: enabled, disabled, or paused
		State string `yaml:"state"`
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
		// State is the state of visibility archival either: enabled, disabled, or paused
		State string `yaml:"state"`
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

	// PublicClient is config for connecting to temporal frontend
	PublicClient struct {
		// HostPort is the host port to connect on. Host can be DNS name
		HostPort string `yaml:"hostPort" validate:"nonzero"`
		// interval to refresh DNS. Default to 10s
		RefreshInterval time.Duration `yaml:"RefreshInterval"`
	}

	// NamespaceDefaults is the default config for each namespace
	NamespaceDefaults struct {
		// Archival is the default archival config for each namespace
		Archival ArchivalNamespaceDefaults `yaml:"archival"`
	}

	// ArchivalNamespaceDefaults is the default archival config for each namespace
	ArchivalNamespaceDefaults struct {
		// History is the namespace default history archival config for each namespace
		History HistoryArchivalNamespaceDefaults `yaml:"history"`
		// Visibility is the namespace default visibility archival config for each namespace
		Visibility VisibilityArchivalNamespaceDefaults `yaml:"visibility"`
	}

	// HistoryArchivalNamespaceDefaults is the default history archival config for each namespace
	HistoryArchivalNamespaceDefaults struct {
		// State is the namespace default state of history archival: enabled or disabled
		State string `yaml:"state"`
		// URI is the namespace default URI for history archiver
		URI string `yaml:"URI"`
	}

	// VisibilityArchivalNamespaceDefaults is the default visibility archival config for each namespace
	VisibilityArchivalNamespaceDefaults struct {
		// State is the namespace default state of visibility archival: enabled or disabled
		State string `yaml:"state"`
		// URI is the namespace default URI for visibility archiver
		URI string `yaml:"URI"`
	}
)

// Validate validates this config
func (c *Config) Validate() error {
	if err := c.Persistence.Validate(); err != nil {
		return err
	}

	if err := c.Archival.Validate(&c.NamespaceDefaults.Archival); err != nil {
		return err
	}

	return nil
}

// String converts the config object into a string
func (c *Config) String() string {
	out, _ := json.MarshalIndent(c, "", "    ")
	return string(out)
}

func (r *GroupTLS) IsEnabled() bool {
	return r.Server.KeyFile != ""
}
