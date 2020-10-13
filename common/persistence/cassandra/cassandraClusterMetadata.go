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

package cassandra

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/cassandra"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/service/config"
)

const constMetadataPartition = 0
const constMembershipPartition = 0

const (
	// ****** CLUSTER_METADATA TABLE ******
	// metadata_partition is constant and PK, this should only return one row.
	templateGetImmutableClusterMetadata = `SELECT immutable_data, immutable_data_encoding FROM 
cluster_metadata 
WHERE metadata_partition = ?`

	immutablePayloadFieldName = `immutable_data`

	immutableEncodingFieldName = immutablePayloadFieldName + `_encoding`

	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	templateGetClusterMetadata = `SELECT data, data_encoding, immutable_data, immutable_data_encoding, version FROM cluster_metadata WHERE metadata_partition = ?`

	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	templateCreateClusterMetadata = `INSERT INTO cluster_metadata (metadata_partition, data, data_encoding, immutable_data, immutable_data_encoding, version) VALUES(?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateClusterMetadata = `UPDATE cluster_metadata SET data = ?, data_encoding = ?, version = ? WHERE metadata_partition = ? IF version = ?`
	// TODO(vitarb): needed only to support legacy records in the immutable metadata, remove after 1.1 release.
	templateInitializeVersionIfNull = `UPDATE cluster_metadata SET version = 1 WHERE metadata_partition = ? IF version = NULL`

	// ****** CLUSTER_MEMBERSHIP TABLE ******
	templateUpsertActiveClusterMembership = `INSERT INTO 
cluster_membership (membership_partition, host_id, rpc_address, rpc_port, role, session_start, last_heartbeat) 
VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?`

	templateGetClusterMembership = `SELECT host_id, rpc_address, rpc_port, role, session_start, last_heartbeat, toTimestamp(now()) as now, TTL(session_start) as ttl_sec FROM
cluster_membership 
WHERE membership_partition = ?`

	templateWithRoleSuffix           = ` AND role = ?`
	templateWithHeartbeatSinceSuffix = ` AND last_heartbeat > ?`
	templateWithRPCAddressSuffix     = ` AND rpc_address = ?`
	templateWithHostIDSuffix         = ` AND host_id = ?`
	templateAllowFiltering           = ` ALLOW FILTERING`
	templateWithSessionSuffix        = ` AND session_start > ?`
)

type (
	cassandraClusterMetadata struct {
		logger log.Logger
		*cassandraStore
	}
)

var _ p.ClusterMetadataStore = (*cassandraClusterMetadata)(nil)

// newClusterMetadataInstance is used to create an instance of ClusterMetadataStore implementation
func newClusterMetadataInstance(cfg config.Cassandra, logger log.Logger) (p.ClusterMetadataStore, error) {
	cluster, err := cassandra.NewCassandraCluster(cfg)
	if err != nil {
		return nil, fmt.Errorf("create cassandra cluster from config: %w", err)
	}
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = cfg.Consistency.GetConsistency()
	cluster.SerialConsistency = cfg.Consistency.GetSerialConsistency()
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create cassandra session from cluster: %w", err)
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

func (m *cassandraClusterMetadata) convertPreviousMapToInitializeResponse(previous map[string]interface{}) (*p.InternalInitializeImmutableClusterMetadataResponse, error) {
	// First, check if fields we are looking for is in previous map and verify we can type assert successfully
	rawImData, ok := previous[immutablePayloadFieldName]
	if !ok {
		return nil, newFieldNotFoundError(immutablePayloadFieldName, previous)
	}

	imData, ok := rawImData.([]byte)
	if !ok {
		var byteSliceType []byte
		return nil, newPersistedTypeMismatchError(immutablePayloadFieldName, byteSliceType, rawImData, previous)
	}

	rawImDataEncoding, ok := previous[immutableEncodingFieldName]
	if !ok {
		return nil, newFieldNotFoundError(immutableEncodingFieldName, previous)
	}

	imDataEncoding, ok := rawImDataEncoding.(string)
	if !ok {
		return nil, newPersistedTypeMismatchError(immutableEncodingFieldName, "", rawImDataEncoding, previous)
	}

	return &p.InternalInitializeImmutableClusterMetadataResponse{
		PersistedImmutableMetadata: p.NewDataBlob(imData, imDataEncoding),
		RequestApplied:             false,
	}, nil
}

func (m *cassandraClusterMetadata) GetClusterMetadata() (*p.InternalGetClusterMetadataResponse, error) {
	query := m.session.Query(templateGetClusterMetadata, constMetadataPartition)
	var clusterMetadata []byte
	var encoding string
	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	var immutableMetadata []byte
	var immutableMetadataEncoding string
	var version int64
	err := query.Scan(&clusterMetadata, &encoding, &immutableMetadata, &immutableMetadataEncoding, &version)
	if err != nil {
		return nil, convertCommonErrors("GetClusterMetadata", err)
	}
	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	if clusterMetadata == nil {
		clusterMetadata = immutableMetadata
		encoding = immutableMetadataEncoding
		// Version can only be 0 for legacy records that have NULL version in the DB.
		// In this case we want to initialize version with a value 1 so that conditional updates can proceed successfully.
		if version == 0 {
			applied, err := m.initializeClusterMetadataVersion()
			if err != nil {
				return nil, convertCommonErrors("GetClusterMetadata", err)
			}
			if !applied {
				return nil, serviceerror.NewInternal("GetClusterMetadata was unable to initialize version for the legacy record.")
			}
			version = 1
		}
	}
	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: p.NewDataBlob(clusterMetadata, encoding),
		Version:         version,
	}, nil
}

func (m *cassandraClusterMetadata) SaveClusterMetadata(request *p.InternalSaveClusterMetadataRequest) (bool, error) {
	// TODO(vitarb): immutable metadata is needed for backward compatibility only, remove after 1.1 release.
	query := m.session.Query(templateCreateClusterMetadata,
		constMembershipPartition, request.ClusterMetadata.Data, request.ClusterMetadata.Encoding.String(), request.ClusterMetadata.Data, request.ClusterMetadata.Encoding.String(), 1)
	if request.Version > 0 {
		query = m.session.Query(templateUpdateClusterMetadata,
			request.ClusterMetadata.Data, request.ClusterMetadata.Encoding.String(), request.Version+1, constMembershipPartition, request.Version)
	}
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return false, convertCommonErrors("SaveClusterMetadata", err)
	}
	if !applied {
		return false, serviceerror.NewInternal("SaveClusterMetadata operation encountered concurrent write.")
	}
	return true, nil
}

func (m *cassandraClusterMetadata) initializeClusterMetadataVersion() (bool, error) {
	query := m.session.Query(templateInitializeVersionIfNull, constMetadataPartition)
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return false, convertCommonErrors("SaveClusterMetadata", err)
	}
	if !applied {
		return false, serviceerror.NewInternal("SaveClusterMetadata operation encountered concurrent write.")
	}
	return true, nil
}

func (m *cassandraClusterMetadata) GetClusterMembers(request *p.GetClusterMembersRequest) (*p.GetClusterMembersResponse, error) {
	var queryString strings.Builder
	var operands []interface{}
	queryString.WriteString(templateGetClusterMembership)
	operands = append(operands, constMembershipPartition)

	if request.HostIDEquals != nil {
		queryString.WriteString(templateWithHostIDSuffix)
		operands = append(operands, []byte(request.HostIDEquals))
	}

	if request.RPCAddressEquals != nil {
		queryString.WriteString(templateWithRPCAddressSuffix)
		operands = append(operands, request.RPCAddressEquals)
	}

	if request.RoleEquals != p.All {
		queryString.WriteString(templateWithRoleSuffix)
		operands = append(operands, request.RoleEquals)
	}

	if !request.SessionStartedAfter.IsZero() {
		queryString.WriteString(templateWithSessionSuffix)
		operands = append(operands, request.SessionStartedAfter)
	}

	// LastHeartbeat needs to be the last one as it needs AllowFilteringSuffix
	if request.LastHeartbeatWithin > 0 {
		queryString.WriteString(templateWithHeartbeatSinceSuffix)
		operands = append(operands, time.Now().UTC().Add(-request.LastHeartbeatWithin))
	}

	queryString.WriteString(templateAllowFiltering)
	query := m.session.Query(queryString.String(), operands...)

	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	if iter == nil {
		return nil, serviceerror.NewInternal("GetClusterMembers operation failed.  Not able to create query iterator.")
	}

	rowCount := iter.NumRows()
	clusterMembers := make([]*p.ClusterMember, 0, rowCount)

	cqlHostID := make([]byte, 0, 16)
	rpcAddress := net.IP{}
	rpcPort := uint16(0)
	role := p.All
	sessionStart := time.Time{}
	lastHeartbeat := time.Time{}
	ttl := 0
	cassNow := time.Time{}

	for iter.Scan(&cqlHostID, &rpcAddress, &rpcPort, &role, &sessionStart, &lastHeartbeat, &cassNow, &ttl) {
		member := p.ClusterMember{
			HostID:        uuid.UUID(cqlHostID),
			RPCAddress:    rpcAddress,
			RPCPort:       rpcPort,
			Role:          role,
			SessionStart:  sessionStart,
			LastHeartbeat: lastHeartbeat,
			RecordExpiry:  cassNow.UTC().Add(time.Second * time.Duration(ttl)),
		}
		clusterMembers = append(clusterMembers, &member)
	}

	pagingToken := iter.PageState()
	var pagingTokenCopy []byte
	// contract of this API expect nil as pagination token instead of empty byte array
	if len(pagingToken) > 0 {
		pagingTokenCopy = make([]byte, len(pagingToken))
		copy(pagingTokenCopy, pagingToken)
	}

	if err := iter.Close(); err != nil {
		return nil, convertCommonErrors("GetClusterMembers", err)
	}

	return &p.GetClusterMembersResponse{ActiveMembers: clusterMembers, NextPageToken: pagingTokenCopy}, nil
}

func (m *cassandraClusterMetadata) UpsertClusterMembership(request *p.UpsertClusterMembershipRequest) error {
	query := m.session.Query(templateUpsertActiveClusterMembership, constMembershipPartition, []byte(request.HostID),
		request.RPCAddress, request.RPCPort, request.Role, request.SessionStart, time.Now().UTC(), int64(request.RecordExpiry.Seconds()))
	err := query.Exec()

	if err != nil {
		return convertCommonErrors("UpsertClusterMembership", err)
	}

	return nil
}

func (m *cassandraClusterMetadata) PruneClusterMembership(request *p.PruneClusterMembershipRequest) error {
	return nil
}
