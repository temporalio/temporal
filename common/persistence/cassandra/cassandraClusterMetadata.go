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
	"net"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cassandra"
	"github.com/temporalio/temporal/common/log"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/config"
)

const constMetadataPartition = 0
const constMembershipPartition = 0

const (
	// ****** CLUSTER_METADATA TABLE ******
	// This should insert the row only if it does not exist. Otherwise noop.
	templateInitImmutableClusterMetadata = `INSERT INTO 
cluster_metadata (metadata_partition, immutable_data, immutable_data_encoding) 
VALUES(?, ?, ?) IF NOT EXISTS`

	// metadata_partition is constant and PK, this should only return one row.
	templateGetImmutableClusterMetadata = `SELECT immutable_data, immutable_data_encoding FROM 
cluster_metadata 
WHERE metadata_partition = ?`

	immutablePayloadFieldName = `immutable_data`

	immutableEncodingFieldName = immutablePayloadFieldName + `_encoding`

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
		return nil, convertCommonErrors("InitializeImmutableClusterMetadata", err)
	}

	if !applied {
		// We didn't apply, parse the previous row
		return m.convertPreviousMapToInitializeResponse(previous)
	}

	// If we applied, return the request back as the persisted data
	return &p.InternalInitializeImmutableClusterMetadataResponse{
		PersistedImmutableMetadata: request.ImmutableClusterMetadata,
		RequestApplied:             true,
	}, nil
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
		PersistedImmutableMetadata: p.NewDataBlob(imData, common.EncodingType(imDataEncoding)),
		RequestApplied:             false,
	}, nil
}

func (m *cassandraClusterMetadata) GetImmutableClusterMetadata() (*p.InternalGetImmutableClusterMetadataResponse, error) {
	query := m.session.Query(templateGetImmutableClusterMetadata, constMetadataPartition)
	var immutableMetadata []byte
	var encoding string
	err := query.Scan(&immutableMetadata, &encoding)
	if err != nil {
		return nil, convertCommonErrors("GetImmutableClusterMetadata", err)
	}

	return &p.InternalGetImmutableClusterMetadataResponse{
		ImmutableClusterMetadata: p.NewDataBlob(immutableMetadata, common.EncodingType(encoding)),
	}, nil
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

	pagingToken := iter.PageState()

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

	if err := iter.Close(); err != nil {
		return nil, convertCommonErrors("GetClusterMembers", err)
	}

	if len(clusterMembers) == 0 {
		pagingToken = nil
	}

	return &p.GetClusterMembersResponse{ActiveMembers: clusterMembers, NextPageToken: pagingToken}, nil
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
