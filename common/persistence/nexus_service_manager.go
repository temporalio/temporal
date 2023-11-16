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

package persistence

import (
	"context"

	"go.temporal.io/api/serviceerror"
)

type (
	nexusServiceManagerImpl struct {
		persistence NexusServiceStore
	}
)

var _ NexusServiceManager = (*nexusServiceManagerImpl)(nil)

func NewNexusServiceManager(
	persistence NexusServiceStore,
) NexusServiceManager {
	return &nexusServiceManagerImpl{
		persistence: persistence,
	}
}

func (m *nexusServiceManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *nexusServiceManagerImpl) Close() {
	m.persistence.Close()
}

func (m *nexusServiceManagerImpl) GetNexusService(
	ctx context.Context,
	request *GetNexusServiceRequest,
) (*GetNexusServiceResponse, error) {
	return nil, serviceerror.NewUnimplemented("NexusServiceManager.GetNexusService() is unimplemented")
}

func (m *nexusServiceManagerImpl) ListNexusServices(
	ctx context.Context,
	request *ListNexusServicesRequest,
) (*ListNexusServicesResponse, error) {
	return nil, serviceerror.NewUnimplemented("NexusServiceManager.ListNexusServices() is unimplemented")
}

func (m *nexusServiceManagerImpl) CreateOrUpdateNexusService(
	ctx context.Context,
	request *CreateOrUpdateNexusServiceRequest,
) error {
	return serviceerror.NewUnimplemented("NexusServiceManager.CreateOrUpdateNexusService() is unimplemented")
}

func (m *nexusServiceManagerImpl) DeleteNexusService(
	ctx context.Context,
	request *DeleteNexusServiceRequest,
) error {
	return serviceerror.NewUnimplemented("NexusServiceManager.DeleteNexusService() is unimplemented")
}
