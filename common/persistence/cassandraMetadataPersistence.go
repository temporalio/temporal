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

package persistence

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

const (
	templateDomainType = `{` +
		`id: ?, ` +
		`name: ?, ` +
		`status: ?, ` +
		`description: ?, ` +
		`owner_email: ?` +
		`}`

	templateDomainConfigType = `{` +
		`retention: ?, ` +
		`emit_metric: ?` +
		`}`

	templateCreateDomainQuery = `INSERT INTO domains (` +
		`id, domain, config) ` +
		`VALUES(?, ` + templateDomainType + `, ` + templateDomainConfigType + `)`

	templateCreateDomainByNameQuery = `INSERT INTO domains_by_name (` +
		`name, domain, config) ` +
		`VALUES(?, ` + templateDomainType + `, ` + templateDomainConfigType + `) IF NOT EXISTS`

	templateGetDomainQuery = `SELECT domain.id, domain.name, domain.status, domain.description, domain.owner_email, ` +
		`config.retention, config.emit_metric ` +
		`FROM domains ` +
		`WHERE id = ?`

	templateGetDomainByNameQuery = `SELECT domain.id, domain.name, domain.status, domain.description, ` +
		`domain.owner_email, config.retention, config.emit_metric ` +
		`FROM domains_by_name ` +
		`WHERE name = ?`

	templateUpdateDomainQuery = `UPDATE domains ` +
		`SET domain = ` + templateDomainType + `, ` +
		`config = ` + templateDomainConfigType +  ` ` +
		`WHERE id = ?`

	templateUpdateDomainByNameQuery = `UPDATE domains_by_name ` +
		`SET domain = ` + templateDomainType + `, ` +
		`config = ` + templateDomainConfigType +  ` ` +
		`WHERE name = ?`

	templateDeleteDomainQuery = `DELETE FROM domains ` +
		`WHERE id = ?`

	templateDeleteDomainByNameQuery = `DELETE FROM domains_by_name ` +
		`WHERE name = ?`
)

type (
	cassandraMetadataPersistence struct {
		session *gocql.Session
		logger  bark.Logger
	}
)

// NewCassandraHistoryPersistence is used to create an instance of HistoryManager implementation
func NewCassandraMetadataPersistence(hosts string, dc string, keyspace string, logger bark.Logger) (MetadataManager,
	error) {
	cluster := common.NewCassandraCluster(hosts, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraMetadataPersistence{session: session, logger: logger}, nil
}

// Cassandra does not support conditional updates across multiple tables.  For this reason we have to first insert into
// 'Domains' table and then do a conditional insert into domains_by_name table.  If the conditional write fails we
// delete the orphaned entry from domains table.  There is a chance delete entry could fail and we never delete the
// orphaned entry from domains table.  We might need a background job to delete those orphaned record.
func (m *cassandraMetadataPersistence) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	domainUUID := uuid.New()
	if err := m.session.Query(templateCreateDomainQuery,
		domainUUID,
		domainUUID,
		request.Name,
		request.Status,
		request.Description,
		request.OwnerEmail,
		request.Retention,
		request.EmitMetric).Exec(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Inserting into domains table. Error: %v", err),
		}
	}

	query := m.session.Query(templateCreateDomainByNameQuery,
		request.Name,
		domainUUID,
		request.Name,
		request.Status,
		request.Description,
		request.OwnerEmail,
		request.Retention,
		request.EmitMetric)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Inserting into domains_by_name table. Error: %v", err),
		}
	}

	if !applied {
		// Domain already exist.  Delete orphan domain record before returning back to user
		if err = m.session.Query(templateDeleteDomainQuery, domainUUID).Exec(); err != nil {
			m.logger.Warnf("Unable to delete orphan domain record. Error: %v", err)
		}

		if domain, ok := previous["domain"].(map[string]interface{}); ok {
			msg := fmt.Sprintf("Domain already exists.  DomainId: %v", domain["id"])
			return nil, &workflow.DomainAlreadyExistsError{
				Message: msg,
			}
		}

		return nil, &workflow.DomainAlreadyExistsError{
			Message: fmt.Sprintf("CreateDomain operation failed because of conditional failure."),
		}
	}

	return &CreateDomainResponse{ID: domainUUID}, nil
}

func (m *cassandraMetadataPersistence) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	var query *gocql.Query
	var err error
	info := &DomainInfo{}
	config := &DomainConfig{}
	if len(request.ID) > 0 {
		if len(request.Name) > 0 {
			return nil, &workflow.BadRequestError{
				Message: "GetDomain operation failed.  Both ID and Name specified in request.",
			}
		}

		query = m.session.Query(templateGetDomainQuery,
			request.ID)
		err = query.Scan(
			&info.ID,
			&info.Name,
			&info.Status,
			&info.Description,
			&info.OwnerEmail,
			&config.Retention,
			&config.EmitMetric)
	} else if len(request.Name) > 0 {
		query = m.session.Query(templateGetDomainByNameQuery,
			request.Name)
		err = query.Scan(
			&info.ID,
			&info.Name,
			&info.Status,
			&info.Description,
			&info.OwnerEmail,
			&config.Retention,
			&config.EmitMetric)
	} else {
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name are empty.",
		}
	}

	if err != nil {
		if err == gocql.ErrNotFound {
			var d string
			if len(request.ID) > 0 {
				d = request.ID
			} else {
				d = request.Name
			}

			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Domain %s does not exist.", d),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetDomain operation failed. Error %v", err),
		}
	}

	return &GetDomainResponse{
		Info:   info,
		Config: config,
	}, nil
}

func (m *cassandraMetadataPersistence) UpdateDomain(request *UpdateDomainRequest) error {
	batch := m.session.NewBatch(gocql.LoggedBatch)

	batch.Query(templateUpdateDomainQuery,
		request.Info.ID,
		request.Info.Name,
		request.Info.Status,
		request.Info.Description,
		request.Info.OwnerEmail,
		request.Config.Retention,
		request.Config.EmitMetric,
		request.Info.ID)

	batch.Query(templateUpdateDomainByNameQuery,
		request.Info.ID,
		request.Info.Name,
		request.Info.Status,
		request.Info.Description,
		request.Info.OwnerEmail,
		request.Config.Retention,
		request.Config.EmitMetric,
		request.Info.Name)

	if err := m.session.ExecuteBatch(batch); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Error %v", err),
		}
	}

	return nil
}

func (m *cassandraMetadataPersistence) DeleteDomain(request *DeleteDomainRequest) error {
	query := m.session.Query(templateDeleteDomainQuery,
		request.ID)

	if err := query.Exec(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteDomain operation failed. Error %v", err),
		}
	}

	return nil
}

func (m *cassandraMetadataPersistence) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	query := m.session.Query(templateDeleteDomainByNameQuery,
		request.Name)

	if err := query.Exec(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteDomainByName operation failed. Error %v", err),
		}
	}

	return nil
}
