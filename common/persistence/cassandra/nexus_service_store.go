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
	templateCreateTableVersion = `INSERT INTO nexus_incoming_services(partition, type, service_id, version) VALUES (?, ?, ?, ?) IF NOT EXISTS`
	templateGetTableVersion    = `SELECT version FROM nexus_incoming_services WHERE partition = ? AND type = ? AND service_id = ?`
	templateUpdateTableVersion = `UPDATE nexus_incoming_services SET version = ? WHERE partition = ? AND type = ? AND service_id = ? IF version = ?`

	// incoming service templates
	templateCreateIncomingServiceQuery         = `INSERT INTO nexus_incoming_services(partition, type, service_id, data, data_encoding, version) VALUES(?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateBaseListIncomingServicesQuery      = `SELECT service_id, data, data_encoding, version FROM nexus_incoming_services WHERE partition = ?`
	templateListIncomingServicesQuery          = templateBaseListIncomingServicesQuery + ` AND type = ?`
	templateListIncomingServicesFirstPageQuery = templateBaseListIncomingServicesQuery + ` ORDER BY type ASC`
	templateUpdateIncomingServiceQuery         = `UPDATE nexus_incoming_services SET data = ?, data_encoding = ?, version = ? WHERE partition = ? AND type = ? AND service_id = ? IF version = ?`
	templateDeleteIncomingServiceQuery         = `DELETE FROM nexus_incoming_services WHERE partition = ? AND type = ? AND service_id = ?`
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
	request *p.InternalCreateOrUpdateNexusIncomingServiceRequest,
) error {
	batch := s.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)

	if request.Service.Version == 0 {
		batch.Query(templateCreateIncomingServiceQuery,
			constServicesPartition,
			rowTypeIncomingNexusService,
			request.Service.ServiceID,
			request.Service.Data.Data,
			request.Service.Data.EncodingType.String(),
			0,
		)
	} else {
		batch.Query(templateUpdateIncomingServiceQuery,
			request.Service.Data.Data,
			request.Service.Data.EncodingType.String(),
			request.Service.Version+1,
			constServicesPartition,
			rowTypeIncomingNexusService,
			request.Service.ServiceID,
			request.Service.Version,
		)
	}

	if request.LastKnownTableVersion == 0 {
		batch.Query(templateCreateTableVersion,
			constServicesPartition,
			rowTypePartitionStatus,
			constTableVersionServiceID,
			0)
	} else {
		batch.Query(templateUpdateTableVersion,
			request.LastKnownTableVersion+1,
			constServicesPartition,
			rowTypePartitionStatus,
			constTableVersionServiceID,
			request.LastKnownTableVersion)
	}

	previous := make(map[string]interface{})
	applied, iter, err := s.session.MapExecuteBatchCAS(batch, previous)

	if err != nil {
		return gocql.ConvertError("CreateOrUpdateNexusIncomingService", err)
	}

	err = iter.Close()
	if err != nil {
		return gocql.ConvertError("CreateOrUpdateNexusIncomingService", err)
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return fmt.Errorf("%w: ID=%v, TableVersion=%v ServiceVersion=%v, columns=(%v)",
			ErrCreateOrUpdateIncomingServiceFailed,
			request.Service.ServiceID,
			request.LastKnownTableVersion,
			request.Service.Version,
			strings.Join(columns, ","))
	}

	return nil
}

func (s *NexusServiceStore) ListNexusIncomingServices(
	ctx context.Context,
	request *p.InternalListNexusIncomingServicesRequest,
) (*p.InternalListNexusIncomingServicesResponse, error) {
	if request.LastKnownTableVersion == UnknownTableVersion {
		return s.listFirstPageWithVersion(ctx, request)
	}

	response := &p.InternalListNexusIncomingServicesResponse{}

	query := s.session.Query(templateListIncomingServicesQuery, constServicesPartition, rowTypeIncomingNexusService).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	services, err := getServiceList(iter)
	if err != nil {
		return nil, err
	}
	response.Services = services

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListNexusIncomingServices operation failed. Error: %v", err))
	}

	currentTableVersion, err := s.getTableVersion(ctx)
	if err != nil {
		return nil, err
	}

	response.TableVersion = currentTableVersion

	if request.LastKnownTableVersion != currentTableVersion {
		// If table has been updated during pagination, throw error to indicate caller must start over
		return nil, fmt.Errorf("%w. Provided table version: %v Current table version: %v",
			ErrTableVersionConflict,
			request.LastKnownTableVersion,
			currentTableVersion)
	}

	return response, nil
}

func (s *NexusServiceStore) DeleteNexusIncomingService(
	ctx context.Context,
	request *p.InternalDeleteNexusIncomingServiceRequest,
) error {
	batch := s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	batch.Query(templateDeleteIncomingServiceQuery,
		constServicesPartition,
		rowTypeIncomingNexusService,
		request.ServiceID)

	batch.Query(templateUpdateTableVersion,
		request.LastKnownTableVersion+1,
		constServicesPartition,
		rowTypePartitionStatus,
		constTableVersionServiceID,
		request.LastKnownTableVersion)

	err := s.session.ExecuteBatch(batch)
	if err != nil {
		return gocql.ConvertError("DeleteNexusIncomingService", err)
	}

	return nil
}

func (s *NexusServiceStore) listFirstPageWithVersion(
	ctx context.Context,
	request *p.InternalListNexusIncomingServicesRequest,
) (*p.InternalListNexusIncomingServicesResponse, error) {
	response := &p.InternalListNexusIncomingServicesResponse{}

	query := s.session.Query(templateListIncomingServicesFirstPageQuery, constServicesPartition).WithContext(ctx)
	iter := query.PageSize(request.PageSize).Iter()

	partitionStateRow := make(map[string]interface{})
	iter.MapScan(partitionStateRow)
	tableVersion, err := getTypedFieldFromRow[int64]("version", partitionStateRow)
	if err != nil {
		return nil, err
	}
	response.TableVersion = tableVersion

	services, err := getServiceList(iter)
	if err != nil {
		return nil, err
	}
	response.Services = services

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListNexusIncomingServices operation failed. Error: %v", err))
	}

	return response, nil
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

func getServiceList(iter gocql.Iter) ([]p.InternalNexusIncomingService, error) {
	var services []p.InternalNexusIncomingService

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

		services = append(services, p.InternalNexusIncomingService{
			ServiceID: serviceID,
			Version:   version,
			Data:      p.NewDataBlob(data, dataEncoding),
		})

		row = make(map[string]interface{})
	}

	return services, nil
}
