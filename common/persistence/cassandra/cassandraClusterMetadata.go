// Copyright (c) 2020 Temporal Technologies, Inc.
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

	"github.com/gocql/gocql"

	workflow "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cassandra"
	"github.com/temporalio/temporal/common/log"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/config"
)

const constMetadataPartition = 0

const (
	// This should insert the row only if it does not exist. Otherwise noop.
	templateInitImmutableClusterMetadata = `INSERT INTO 
cluster_metadata (metadata_partition, immutable_data, immutable_data_encoding) 
VALUES(?, ?, ?) IF NOT EXISTS`

	// metadata_partition is constant and PK, this should only return one row.
	templateGetImmutableClusterMetadata = `SELECT immutable_data, immutable_data_encoding FROM 
cluster_metadata WHERE metadata_partition = ?`
)

type (
	cassandraClusterMetadata struct {
		logger log.Logger
		*cassandraStore
	}
)

var _ p.ClusterMetadataStore = (*cassandraClusterMetadata)(nil)

// newMetadataPersistenceV2 is used to create an instance of HistoryManager implementation
func newClusterMetadataInstance(cfg config.Cassandra, logger log.Logger) (p.ClusterMetadataStore, error) {
	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraClusterMetadata{
		cassandraStore: &cassandraStore{session: session, logger: logger},
		logger:         logger,
	}, nil
}

// Close releases the resources held by this object
func (m *cassandraClusterMetadata) Close() {
	if m.session != nil {
		m.session.Close()
	}
}

func (m *cassandraClusterMetadata) InitializeImmutableClusterMetadata(
	request *p.InternalInitializeImmutableClusterMetadataRequest) (*p.InternalInitializeImmutableClusterMetadataResponse, error) {
	query := m.session.Query(templateInitImmutableClusterMetadata, constMetadataPartition,
		request.ImmutableClusterMetadata.Data, request.ImmutableClusterMetadata.Encoding)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)

	if err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CreateImmutableMetadataEntryIfNoneExists operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateImmutableMetadataEntryIfNoneExists operation failed. Error: %v", err),
		}
	}

	if !applied {
		// We didn't apply, parse the previous row
		return &p.InternalInitializeImmutableClusterMetadataResponse{
			PersistedImmutableMetadata: p.NewDataBlob(previous["immutable_data"].([]byte),
				common.EncodingType(previous["immutable_data_encoding"].(string))),
			RequestApplied: false,
		}, nil
	}

	// If we applied, return the request back as the persisted data
	return &p.InternalInitializeImmutableClusterMetadataResponse{
		PersistedImmutableMetadata: request.ImmutableClusterMetadata,
		RequestApplied:             true,
	}, nil
}

func (m *cassandraClusterMetadata) GetImmutableClusterMetadata() (*p.InternalGetImmutableClusterMetadataResponse, error) {
	query := m.session.Query(templateGetImmutableClusterMetadata, constMetadataPartition)
	var immutableMetadata []byte
	var encoding string
	err := query.Scan(&immutableMetadata, &encoding)
	if err != nil {
		if err == gocql.ErrNotFound {
			return &p.InternalGetImmutableClusterMetadataResponse{}, nil
		}

		return nil, fmt.Errorf("failed to get immutable cluster metadata: %v", err)
	}

	return &p.InternalGetImmutableClusterMetadataResponse{
		ImmutableClusterMetadata: p.NewDataBlob(immutableMetadata, common.EncodingType(encoding)),
	}, nil
}
