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
	"context"
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	UnknownTableVersion = 0

	constServicesPartition     = 0
	constTableVersionServiceID = 0
)

const (
	rowTypePartitionStatus      = iota
	rowTypeIncomingNexusService = iota
)

const (
	// table templates
	templateGetTableVersion    = `SELECT version FROM nexus_incoming_services WHERE partition = ? AND type = ? AND service_id = ?`
	templateUpdateTableVersion = `UPDATE nexus_incoming_services SET version = toTimestamp(now()) WHERE partition = ? AND type = ? AND service_id = ?`

	// incoming service templates
	templateCreateIncomingServiceQuery = `INSERT INTO nexus_incoming_services(partition, type, service_id, data, data_encoding, version) VALUES(?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateListIncomingServicesQuery  = `SELECT data, data_encoding, version FROM nexus_incoming_services WHERE partition = ? AND type = ?`
	templateGetIncomingServiceQuery    = templateListIncomingServicesQuery + ` AND service_id = ?`
	templateUpdateIncomingServiceQuery = `UPDATE nexus_incoming_services SET data = ?, data_encoding = ?, version = ? WHERE partition = ? AND type = ? AND service_id = ? IF version = ?`
	templateDeleteIncomingServiceQuery = `DELETE FROM nexus_incoming_services WHERE partition = ? AND type = ? AND service_id = ?`
)

var (
	ErrTableVersionConflict = &p.ConditionFailedError{
		Msg: "nexus incoming services table version mismatch",
	}
	ErrCreateOrUpdateIncomingServiceFailed = &p.ConditionFailedError{
		Msg: "error creating or updating nexus incoming service",
	}
)

type (
	NexusServiceStore struct {
		session gocql.Session
		logger  log.Logger
	}
)

func NewNexusServiceStore(
	session gocql.Session,
	logger log.Logger,
) p.NexusServiceStore {
	return &NexusServiceStore{
		session: session,
		logger:  logger,
	}
}

func (s *NexusServiceStore) GetName() string {
	return cassandraPersistenceName
}

func (s *NexusServiceStore) Close() {
	if s.session != nil {
		s.session.Close()
	}
}

func (s *NexusServiceStore) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	service *p.InternalNexusIncomingService,
) error {
	batch := s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	if service.Version == 0 {
		batch.Query(templateCreateIncomingServiceQuery,
			constServicesPartition,
			rowTypeIncomingNexusService,
			service.ServiceID,
			service.Data.Data,
			service.Data.EncodingType.String(),
			0,
		)
	} else {
		batch.Query(templateUpdateIncomingServiceQuery,
			service.Data.Data,
			service.Data.EncodingType.String(),
			service.Version+1,
			constServicesPartition,
			rowTypeIncomingNexusService,
			service.ServiceID,
			service.Version,
		)
	}

	batch.Query(templateUpdateTableVersion, constServicesPartition, rowTypePartitionStatus, constTableVersionServiceID)

	previous := make(map[string]interface{})
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, previous)

	if err != nil {
		return gocql.ConvertError("CreateOrUpdateNexusIncomingService", err)
	}

	// We only care about the conflict in the first query
	err = iter.Close()
	if err != nil {
		return gocql.ConvertError("CreateOrUpdateNexusIncomingService", err)
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return fmt.Errorf("%w: ID=%v, version=%v, columns=(%v)",
			ErrCreateOrUpdateIncomingServiceFailed,
			service.ServiceID,
			service.Version,
			strings.Join(columns, ","))
	}

	return nil
}

func (s *NexusServiceStore) GetNexusIncomingService(
	ctx context.Context,
	serviceID string,
) (*p.InternalNexusIncomingService, error) {
	query := s.session.Query(templateGetIncomingServiceQuery,
		constServicesPartition,
		rowTypeIncomingNexusService,
		serviceID,
	).WithContext(ctx)

	var version int64
	var dataBytes []byte
	var dataEncoding string

	if err := query.Scan(&dataBytes, &dataEncoding, &version); err != nil {
		return nil, gocql.ConvertError("GetNexusIncomingService", err)
	}

	return &p.InternalNexusIncomingService{
		ServiceID: serviceID,
		Version:   version,
		Data:      p.NewDataBlob(dataBytes, dataEncoding),
	}, nil
}

func (s *NexusServiceStore) ListNexusIncomingServices(
	ctx context.Context,
	request *p.InternalListNexusIncomingServicesRequest,
) (*p.InternalListNexusIncomingServicesResponse, error) {
	response := &p.InternalListNexusIncomingServicesResponse{}

	currentTableVersion, err := s.getTableVersion(ctx)
	if err != nil {
		return nil, err
	}

	response.TableVersion = currentTableVersion

	if request.LastKnownTableVersion != UnknownTableVersion && request.LastKnownTableVersion != currentTableVersion {
		// If table has been updated during pagination, throw error to indicate caller must start over
		return nil, fmt.Errorf("%w. Provided table version: %v Current table version: %v",
			ErrTableVersionConflict,
			request.LastKnownTableVersion,
			currentTableVersion)
	}

	query := s.session.Query(templateListIncomingServicesQuery, constServicesPartition, rowTypeIncomingNexusService).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	row := make(map[string]interface{})
	for iter.MapScan(row) {
		serviceID, err := getTypedFieldFromRow[string]("service_id", row)
		if err != nil {
			return nil, err
		}
		version, err := getTypedFieldFromRow[int64]("version", row)
		if err != nil {
			return nil, err
		}
		data, err := getTypedFieldFromRow[[]byte]("data", row)
		if err != nil {
			return nil, err
		}
		dataEncoding, err := getTypedFieldFromRow[string]("data_encoding", row)
		if err != nil {
			return nil, err
		}

		response.Services = append(response.Services, p.InternalNexusIncomingService{
			ServiceID: serviceID,
			Version:   version,
			Data:      p.NewDataBlob(data, dataEncoding),
		})

		row = make(map[string]interface{})
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListNexusIncomingServices operation failed. Error: %v", err))
	}

	return response, nil
}

func (s *NexusServiceStore) DeleteNexusIncomingService(
	ctx context.Context,
	serviceID string,
) error {
	batch := s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	batch.Query(templateDeleteIncomingServiceQuery,
		constServicesPartition,
		rowTypeIncomingNexusService,
		serviceID,
	)

	batch.Query(templateUpdateTableVersion, constServicesPartition, rowTypePartitionStatus, constTableVersionServiceID)

	err := s.session.ExecuteBatch(batch)
	if err != nil {
		return gocql.ConvertError("DeleteNexusIncomingService", err)
	}

	return nil
}

func (s *NexusServiceStore) getTableVersion(ctx context.Context) (int64, error) {
	query := s.session.Query(templateGetTableVersion,
		constServicesPartition,
		rowTypePartitionStatus,
		constTableVersionServiceID,
	).WithContext(ctx)

	var version int64
	if err := query.Scan(&version); err != nil {
		return UnknownTableVersion, gocql.ConvertError("GetNexusIncomingServicesTableVersion", err)
	}

	return version, nil
}
