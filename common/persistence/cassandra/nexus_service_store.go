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

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
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

func (s *NexusServiceStore) GetNexusService(
	ctx context.Context,
	name string,
) (*p.InternalGetNexusServiceResponse, error) {
	return nil, serviceerror.NewUnimplemented("Cassandra GetNexusService is not implemented")
}

func (s *NexusServiceStore) ListNexusServices(
	ctx context.Context,
	request *p.InternalListNexusServicesRequest,
) (*p.InternalListNexusServicesResponse, error) {
	return nil, serviceerror.NewUnimplemented("Cassandra ListNexusServices is not implemented")
}

func (s *NexusServiceStore) CreateOrUpdateNexusService(
	ctx context.Context,
	request *p.InternalCreateOrUpdateNexusServiceRequest,
) error {
	return serviceerror.NewUnimplemented("Cassandra CreateOrUpdateNexusService is not implemented")
}

func (s *NexusServiceStore) DeleteNexusService(
	ctx context.Context,
	name string,
) error {
	return serviceerror.NewUnimplemented("Cassandra DeleteNexusService is not implemented")
}
