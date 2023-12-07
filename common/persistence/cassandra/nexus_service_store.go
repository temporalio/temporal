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
	constServicesPartition = 0
)

const (
	rowTypePartitionStatus      = iota
	rowTypeIncomingNexusService = iota
)

const (
	templateCreateIncomingServiceQuery = `INSERT INTO nexus_services(` +
		`services_partition, service_id, type, service_name, namespace_id, task_queue_name, metadata, metadata_encoding, version)` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateListIncomingServicesQuery = `SELECT service_name, namespace_id, task_queue_name, metadata, metadata_encoding, version ` +
		`FROM nexus_services ` +
		`WHERE services_partition = ? AND type = ?`

	templateGetIncomingServiceQuery = templateListIncomingServicesQuery +
		` AND service_id = ?`

	templateUpdateIncomingServiceQuery = `UPDATE nexus_services SET ` +
		`service_name = ?, namespace_id = ?, task_queue_name = ?, metadata = ?, metadata_encoding = ?, version = ? ` +
		`WHERE services_partition = ? AND type = ? AND service_id = ? ` +
		`IF version = ?`

	templateDeleteIncomingServiceQuery = `DELETE FROM nexus_services ` +
		`WHERE services_partition = ? AND type = ? AND service_id = ?`
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
	// TODO: setting up as a batch because soon we will need to maintain a service_name -> service_id mapping
	// in order to populate service registry cache (similar to how namespace registry works)
	batch := s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	if request.Service.Version == 0 {
		batch.Query(templateCreateIncomingServiceQuery,
			constServicesPartition,
			request.ServiceID,
			rowTypeIncomingNexusService,
			request.Service.ServiceName,
			request.Service.NamespaceID,
			request.Service.TaskQueueName,
			request.Service.Metadata.Data,
			request.Service.Metadata.EncodingType.String(),
			0,
		)
	} else {
		batch.Query(templateUpdateIncomingServiceQuery,
			request.Service.ServiceName,
			request.Service.NamespaceID,
			request.Service.TaskQueueName,
			request.Service.Metadata.Data,
			request.Service.Metadata.EncodingType.String(),
			request.Service.Version+1,
			constServicesPartition,
			rowTypeIncomingNexusService,
			request.ServiceID,
			request.Service.Version,
		)
	}

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

		return &p.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create/update Nexus incoming service. name: %v, version: %v, columns: (%v)",
				request.Service.ServiceName, request.Service.Version, strings.Join(columns, ",")),
		}
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

	var serviceName string
	var namespaceID string
	var taskQueueName string
	var metadataBytes []byte
	var metadataEncoding string
	var version int64

	if err := query.Scan(&serviceName, &namespaceID, &taskQueueName, &metadataBytes, &metadataEncoding, &version); err != nil {
		return nil, gocql.ConvertError("GetNexusIncomingService", err)
	}

	return &p.InternalNexusIncomingService{
		ServiceName:   serviceName,
		NamespaceID:   namespaceID,
		TaskQueueName: taskQueueName,
		Metadata:      p.NewDataBlob(metadataBytes, metadataEncoding),
		Version:       version,
	}, nil
}

func (s *NexusServiceStore) ListNexusIncomingServices(
	ctx context.Context,
	request *p.InternalListNexusIncomingServicesRequest,
) (*p.InternalListNexusIncomingServicesResponse, error) {
	query := s.session.Query(templateListIncomingServicesQuery, constServicesPartition, rowTypeIncomingNexusService).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalListNexusIncomingServicesResponse{}
	row := make(map[string]interface{})

	for iter.MapScan(row) {
		serviceName, err := getTypedFieldFromRow[string]("service_name", row)
		if err != nil {
			return nil, err
		}
		namespaceID, err := getTypedFieldFromRow[string]("namespace_id", row)
		if err != nil {
			return nil, err
		}
		taskQueueName, err := getTypedFieldFromRow[string]("task_queue_name", row)
		if err != nil {
			return nil, err
		}
		metadata, err := getTypedFieldFromRow[[]byte]("metadata", row)
		if err != nil {
			return nil, err
		}
		metadataEncoding, err := getTypedFieldFromRow[string]("metadata_encoding", row)
		if err != nil {
			return nil, err
		}
		version, err := getTypedFieldFromRow[int64]("version", row)
		if err != nil {
			return nil, err
		}

		response.Services = append(response.Services, p.InternalNexusIncomingService{
			ServiceName:   serviceName,
			NamespaceID:   namespaceID,
			TaskQueueName: taskQueueName,
			Metadata:      p.NewDataBlob(metadata, metadataEncoding),
			Version:       version,
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
	query := s.session.Query(templateDeleteIncomingServiceQuery,
		constServicesPartition,
		rowTypeIncomingNexusService,
		serviceID,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	_, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("DeleteNexusIncomingService", err)
	}

	return nil
}
