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

package cassandra

import (
	"fmt"
	"regexp"
)

type (
	// BaseConfig is the common config
	// for all of the tasks that work
	// with cassandra
	BaseConfig struct {
		CassHosts    string
		CassPort     int
		CassUser     string
		CassPassword string
		CassKeyspace string
		CassTimeout  int
	}

	// UpdateSchemaConfig holds the config
	// params for executing a UpdateSchemaTask
	UpdateSchemaConfig struct {
		BaseConfig
		TargetVersion string
		SchemaDir     string
		IsDryRun      bool
	}

	// SetupSchemaConfig holds the config
	// params need by the SetupSchemaTask
	SetupSchemaConfig struct {
		BaseConfig
		SchemaFilePath    string
		InitialVersion    string
		Overwrite         bool // overwrite previous data
		DisableVersioning bool // do not use schema versioning
	}

	// CreateKeyspaceConfig holds the config
	// params needed to create a cassandra
	// keyspace
	CreateKeyspaceConfig struct {
		BaseConfig
		ReplicationFactor int
	}

	// ConfigError is an error type that
	// represents a problem with the config
	ConfigError struct {
		msg string
	}
)

const (
	cliOptEndpoint          = "endpoint"
	cliOptPort              = "port"
	cliOptUser              = "user"
	cliOptPassword          = "password"
	cliOptTimeout           = "timeout"
	cliOptKeyspace          = "keyspace"
	cliOptVersion           = "version"
	cliOptSchemaFile        = "schema-file"
	cliOptOverwrite         = "overwrite"
	cliOptDisableVersioning = "disable-versioning"
	cliOptTargetVersion     = "version"
	cliOptDryrun            = "dryrun"
	cliOptSchemaDir         = "schema-dir"
	cliOptReplicationFactor = "replication-factor"
	cliOptQuiet             = "quiet"

	cliFlagEndpoint          = cliOptEndpoint + ", ep"
	cliFlagPort              = cliOptPort + ", p"
	cliFlagUser              = cliOptUser + ", u"
	cliFlagPassword          = cliOptPassword + ", pw"
	cliFlagTimeout           = cliOptTimeout + ", t"
	cliFlagKeyspace          = cliOptKeyspace + ", k"
	cliFlagVersion           = cliOptVersion + ", v"
	cliFlagSchemaFile        = cliOptSchemaFile + ", f"
	cliFlagOverwrite         = cliOptOverwrite + ", o"
	cliFlagDisableVersioning = cliOptDisableVersioning + ", d"
	cliFlagTargetVersion     = cliOptTargetVersion + ", v"
	cliFlagDryrun            = cliOptDryrun + ", y"
	cliFlagSchemaDir         = cliOptSchemaDir + ", d"
	cliFlagReplicationFactor = cliOptReplicationFactor + ", rf"
	cliFlagQuiet             = cliOptQuiet + ", q"
)

var rmspaceRegex = regexp.MustCompile("\\s+")

func newConfigError(msg string) error {
	return &ConfigError{msg: msg}
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("Config Error:%v", e.msg)
}
