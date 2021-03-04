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
	"net"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const constMetadataPartition = 0
const constMembershipPartition = 0

const (
	templateGetClusterMetadata    = `SELECT data, data_encoding, version FROM cluster_metadata WHERE metadata_partition = ?`
	templateCreateClusterMetadata = `INSERT INTO cluster_metadata (metadata_partition, data, data_encoding, version) VALUES(?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateClusterMetadata = `UPDATE cluster_metadata SET data = ?, data_encoding = ?, version = ? WHERE metadata_partition = ? IF version = ?`

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
func newClusterMetadataInstance(
	session gocql.Session,
	logger log.Logger,
) (p.ClusterMetadataStore, error) {

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

func (m *cassandraClusterMetadata) GetClusterMetadata() (*p.InternalGetClusterMetadataResponse, error) {
	query := m.session.Query(templateGetClusterMetadata, constMetadataPartition)
	var clusterMetadata []byte
	var encoding string

	var version int64
	err := query.Scan(&clusterMetadata, &encoding, &version)
	if err != nil {
		return nil, gocql.ConvertError("GetClusterMetadata", err)
	}
	return &p.InternalGetClusterMetadataResponse{
		ClusterMetadata: p.NewDataBlob(clusterMetadata, encoding),
		Version:         version,
	}, nil
}

func (m *cassandraClusterMetadata) SaveClusterMetadata(request *p.InternalSaveClusterMetadataRequest) (bool, error) {
	var query gocql.Query
	if request.Version == 0 {
		query = m.session.Query(
			templateCreateClusterMetadata,
			constMembershipPartition,
			request.ClusterMetadata.Data,
			request.ClusterMetadata.EncodingType.String(),
			1,
		)
	} else {
		query = m.session.Query(
			templateUpdateClusterMetadata,
			request.ClusterMetadata.Data,
			request.ClusterMetadata.EncodingType.String(),
			request.Version+1,
			constMembershipPartition,
			request.Version,
		)
	}

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return false, gocql.ConvertError("SaveClusterMetadata", err)
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

	var clusterMembers []*p.ClusterMember

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
		return nil, gocql.ConvertError("GetClusterMembers", err)
	}

	return &p.GetClusterMembersResponse{ActiveMembers: clusterMembers, NextPageToken: pagingTokenCopy}, nil
}

func (m *cassandraClusterMetadata) UpsertClusterMembership(request *p.UpsertClusterMembershipRequest) error {
	query := m.session.Query(templateUpsertActiveClusterMembership, constMembershipPartition, []byte(request.HostID),
		request.RPCAddress, request.RPCPort, request.Role, request.SessionStart, time.Now().UTC(), int64(request.RecordExpiry.Seconds()))
	err := query.Exec()

	if err != nil {
		return gocql.ConvertError("UpsertClusterMembership", err)
	}

	return nil
}

func (m *cassandraClusterMetadata) PruneClusterMembership(request *p.PruneClusterMembershipRequest) error {
	return nil
}
