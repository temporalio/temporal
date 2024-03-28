// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package tests

import (
	"testing"

	"github.com/google/uuid"
	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/anypb"

	"go.temporal.io/server/api/matchingservice/v1"
	p "go.temporal.io/server/common/persistence"
)

func (s *FunctionalSuite) TestCreateNexusIncomingService_Matching() {
	service := s.createNexusIncomingService(s.T().Name())
	s.Equal(int64(1), service.Version)
	s.Nil(service.LastModifiedTime)
	s.NotNil(service.CreatedTime)
	s.NotEmpty(service.Id)
	s.Equal(service.Spec.Name, s.T().Name())
	s.Equal(service.Spec.Namespace, s.namespace)
	s.Equal("/api/v1/services/"+service.Id, service.UrlPrefix)

	_, err := s.testCluster.GetMatchingClient().CreateNexusIncomingService(NewContext(), &matchingservice.CreateNexusIncomingServiceRequest{
		Spec: &nexus.IncomingServiceSpec{
			Name:      s.T().Name(),
			Namespace: s.namespace,
			TaskQueue: "dont-care",
		},
	})
	var existsErr *serviceerror.AlreadyExists
	s.ErrorAs(err, &existsErr)
}

func (s *FunctionalSuite) TestUpdateNexusIncomingService_Matching() {
	service := s.createNexusIncomingService(s.T().Name())
	type testcase struct {
		name      string
		request   *matchingservice.UpdateNexusIncomingServiceRequest
		assertion func(*matchingservice.UpdateNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid update",
			request: &matchingservice.UpdateNexusIncomingServiceRequest{
				Version: 1,
				Id:      service.Id,
				Spec: &nexus.IncomingServiceSpec{
					Name:      "updated name",
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *matchingservice.UpdateNexusIncomingServiceResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Service)
				s.Equal("/api/v1/services/"+service.Id, service.UrlPrefix)
				s.Equal(int64(2), resp.Service.Version)
				s.Equal("updated name", resp.Service.Spec.Name)
				s.NotNil(resp.Service.LastModifiedTime)
			},
		},
		{
			name: "invalid update: service not found",
			request: &matchingservice.UpdateNexusIncomingServiceRequest{
				Version: 1,
				Id:      "not-found",
				Spec: &nexus.IncomingServiceSpec{
					Name:      "updated name",
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *matchingservice.UpdateNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid update: service version mismatch",
			request: &matchingservice.UpdateNexusIncomingServiceRequest{
				Version: 1,
				Id:      service.Id,
				Spec: &nexus.IncomingServiceSpec{
					Name:      "updated name",
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *matchingservice.UpdateNexusIncomingServiceResponse, err error) {
				var fpErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &fpErr)
			},
		},
	}

	matchingClient := s.testCluster.GetMatchingClient()
	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.UpdateNexusIncomingService(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestDeleteNexusIncomingService_Matching() {
	service := s.createNexusIncomingService("service-to-delete-matching")
	type testcase struct {
		name      string
		serviceId string
		assertion func(*matchingservice.DeleteNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name:      "invalid delete: not found",
			serviceId: "missing-service",
			assertion: func(resp *matchingservice.DeleteNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:      "valid delete",
			serviceId: service.Id,
			assertion: func(resp *matchingservice.DeleteNexusIncomingServiceResponse, err error) {
				s.NoError(err)
			},
		},
	}

	matchingClient := s.testCluster.GetMatchingClient()
	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.DeleteNexusIncomingService(
				NewContext(),
				&matchingservice.DeleteNexusIncomingServiceRequest{
					Id: tc.serviceId,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestListNexusIncomingServices_Matching() {
	// initialize some services
	s.createNexusIncomingService("list-test-service0")
	s.createNexusIncomingService("list-test-service1")
	s.createNexusIncomingService("list-test-service2")

	// get expected table version and services for the course of the tests
	matchingClient := s.testCluster.GetMatchingClient()
	resp, err := matchingClient.ListNexusIncomingServices(
		NewContext(),
		&matchingservice.ListNexusIncomingServicesRequest{
			PageSize:              100,
			LastKnownTableVersion: 0,
			Wait:                  false,
		})
	s.NoError(err)
	s.NotNil(resp)
	tableVersion := resp.TableVersion
	servicesOrdered := resp.Services
	nextPageToken := []byte(servicesOrdered[2].Id)

	type testcase struct {
		name      string
		request   *matchingservice.ListNexusIncomingServicesRequest
		assertion func(*matchingservice.ListNexusIncomingServicesResponse, error)
	}
	testCases := []testcase{
		{
			name: "list nexus incoming services: first_page=true | wait=false | table_version=unknown",
			request: &matchingservice.ListNexusIncomingServicesRequest{
				NextPageToken:         nil,
				LastKnownTableVersion: 0,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.TableVersion)
				s.Equal([]byte(servicesOrdered[2].Id), resp.NextPageToken)
				s.ProtoElementsMatch(resp.Services, servicesOrdered[0:2])
			},
		},
		{
			name: "list nexus incoming services: first_page=true | wait=true | table_version=unknown",
			request: &matchingservice.ListNexusIncomingServicesRequest{
				NextPageToken:         nil,
				LastKnownTableVersion: 0,
				Wait:                  true,
				PageSize:              3,
			},
			assertion: func(resp *matchingservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.TableVersion)
				s.ProtoElementsMatch(resp.Services, servicesOrdered[0:3])
			},
		},
		{
			name: "list nexus incoming services: first_page=false | wait=false | table_version=greater",
			request: &matchingservice.ListNexusIncomingServicesRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion + 1,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusIncomingServicesResponse, err error) {
				var failedPreErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreErr)
			},
		},
		{
			name: "list nexus incoming services: first_page=false | wait=false | table_version=lesser",
			request: &matchingservice.ListNexusIncomingServicesRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion - 1,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusIncomingServicesResponse, err error) {
				var failedPreErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreErr)
			},
		},
		{
			name: "list nexus incoming services: first_page=false | wait=false | table_version=expected",
			request: &matchingservice.ListNexusIncomingServicesRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion,
				Wait:                  false,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion, resp.TableVersion)
				s.ProtoEqual(resp.Services[0], servicesOrdered[2])
			},
		},
		{
			name: "list nexus incoming services: first_page=false | wait=true | table_version=expected",
			request: &matchingservice.ListNexusIncomingServicesRequest{
				NextPageToken:         nextPageToken,
				LastKnownTableVersion: tableVersion,
				Wait:                  true,
				PageSize:              2,
			},
			assertion: func(resp *matchingservice.ListNexusIncomingServicesResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
		{
			name: "list nexus incoming services: first_page=true | wait=true | table_version=expected",
			request: &matchingservice.ListNexusIncomingServicesRequest{
				NextPageToken:         nil,
				LastKnownTableVersion: tableVersion,
				Wait:                  true,
				PageSize:              3,
			},
			assertion: func(resp *matchingservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.Equal(tableVersion+1, resp.TableVersion)
				s.NotNil(resp.NextPageToken)
				s.Len(resp.Services, 3)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			listReqDone := make(chan struct{})
			go func() {
				defer close(listReqDone)
				resp, err := matchingClient.ListNexusIncomingServices(NewContext(), tc.request)
				tc.assertion(resp, err)
			}()
			if tc.request.Wait && tc.request.NextPageToken == nil && tc.request.LastKnownTableVersion != 0 {
				s.createNexusIncomingService("new-service")
			}
			<-listReqDone
		})
	}
}

func (s *FunctionalSuite) TestCreateNexusIncomingService_Operator() {
	type testcase struct {
		name      string
		request   *operatorservice.CreateNexusIncomingServiceRequest
		assertion func(*operatorservice.CreateNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid create",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.T().Name(),
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.NotNil(resp.Service)
				s.Equal(int64(1), resp.Service.Version)
				s.Nil(resp.Service.LastModifiedTime)
				s.NotNil(resp.Service.CreatedTime)
				s.NotEmpty(resp.Service.Id)
				s.Equal(resp.Service.Spec.Name, s.T().Name())
				s.Equal(resp.Service.Spec.Namespace, s.namespace)
				s.Equal("/api/v1/services/"+resp.Service.Id, resp.Service.UrlPrefix)
			},
		},
		{
			name: "invalid: name already in use",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.T().Name(),
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				var existsErr *serviceerror.AlreadyExists
				s.ErrorAs(err, &existsErr)
			},
		},
		{
			name: "invalid: name unset",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service name not set")
			},
		},
		{
			name: "invalid: name too long",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      string(make([]byte, 300)),
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service name exceeds length limit")
			},
		},
		{
			name: "invalid: malformed name",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      "\n```\n",
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service name must match the regex")
			},
		},
		{
			name: "invalid: namespace unset",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.randomizeStr(s.T().Name()),
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service namespace not set")
			},
		},
		{
			name: "invalid: namespace not found",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.randomizeStr(s.T().Name()),
					Namespace: "missing-namespace",
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				var preCondErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &preCondErr)
			},
		},
		{
			name: "invalid: task queue unset",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.randomizeStr(s.T().Name()),
					Namespace: s.namespace,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service task queue not set")
			},
		},
		{
			name: "invalid: task queue too long",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.randomizeStr(s.T().Name()),
					Namespace: s.namespace,
					TaskQueue: string(make([]byte, 1005)),
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service task queue exceeds length limit")
			},
		},
		{
			name: "invalid: task queue starts with reserved prefix",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.randomizeStr(s.T().Name()),
					Namespace: s.namespace,
					TaskQueue: "/_sys/" + s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service task queue begins with reserved prefix")
			},
		},
		{
			name: "invalid: service too large",
			request: &operatorservice.CreateNexusIncomingServiceRequest{
				Spec: &nexus.IncomingServiceSpec{
					Name:      s.randomizeStr(s.T().Name()),
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
					Metadata:  map[string]*anypb.Any{"k1": {Value: make([]byte, 4100)}},
				},
			},
			assertion: func(resp *operatorservice.CreateNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service size exceeds limit")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.CreateNexusIncomingService(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestUpdateNexusIncomingService_Operator() {
	service := s.createNexusIncomingService(s.T().Name())
	type testcase struct {
		name      string
		request   *operatorservice.UpdateNexusIncomingServiceRequest
		assertion func(*operatorservice.UpdateNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid update",
			request: &operatorservice.UpdateNexusIncomingServiceRequest{
				Version: 1,
				Id:      service.Id,
				Spec: &nexus.IncomingServiceSpec{
					Name:      "updated name",
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.UpdateNexusIncomingServiceResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Service)
				s.Equal("/api/v1/services/"+service.Id, service.UrlPrefix)
				s.Equal(int64(2), resp.Service.Version)
				s.Equal("updated name", resp.Service.Spec.Name)
				s.NotNil(resp.Service.LastModifiedTime)
			},
		},
		{
			name: "invalid: service not found",
			request: &operatorservice.UpdateNexusIncomingServiceRequest{
				Version: 1,
				Id:      "not-found",
				Spec: &nexus.IncomingServiceSpec{
					Name:      "updated name",
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.UpdateNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid: service version mismatch",
			request: &operatorservice.UpdateNexusIncomingServiceRequest{
				Version: 1,
				Id:      service.Id,
				Spec: &nexus.IncomingServiceSpec{
					Name:      "updated name",
					Namespace: s.namespace,
					TaskQueue: s.defaultTaskQueue().Name,
				},
			},
			assertion: func(resp *operatorservice.UpdateNexusIncomingServiceResponse, err error) {
				var fpErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &fpErr)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.UpdateNexusIncomingService(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestDeleteNexusIncomingService_Operator() {
	service := s.createNexusIncomingService("service-to-delete-operator")
	type testcase struct {
		name      string
		serviceId string
		assertion func(*operatorservice.DeleteNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name:      "invalid delete: not found",
			serviceId: uuid.NewString(),
			assertion: func(resp *operatorservice.DeleteNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:      "valid delete",
			serviceId: service.Id,
			assertion: func(resp *operatorservice.DeleteNexusIncomingServiceResponse, err error) {
				s.NoError(err)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.DeleteNexusIncomingService(
				NewContext(),
				&operatorservice.DeleteNexusIncomingServiceRequest{
					Id:      tc.serviceId,
					Version: 1,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestListNexusIncomingServices_Operator() {
	// initialize some services
	s.createNexusIncomingService("operator-list-test-service0")
	s.createNexusIncomingService("operator-list-test-service1")
	serviceToFilter := s.createNexusIncomingService("operator-list-test-service2")

	// get ordered services for the course of the tests
	resp, err := s.operatorClient.ListNexusIncomingServices(NewContext(), &operatorservice.ListNexusIncomingServicesRequest{})
	s.NoError(err)
	s.NotNil(resp)
	servicesOrdered := resp.Services

	resp, err = s.operatorClient.ListNexusIncomingServices(NewContext(), &operatorservice.ListNexusIncomingServicesRequest{PageSize: 2})
	s.NoError(err)
	s.NotNil(resp)
	nextPageToken := resp.NextPageToken

	type testcase struct {
		name      string
		request   *operatorservice.ListNexusIncomingServicesRequest
		assertion func(*operatorservice.ListNexusIncomingServicesResponse, error)
	}
	testCases := []testcase{
		{
			name: "list first page",
			request: &operatorservice.ListNexusIncomingServicesRequest{
				NextPageToken: nil,
				PageSize:      2,
			},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.Equal(nextPageToken, resp.NextPageToken)
				s.ProtoElementsMatch(resp.Services, servicesOrdered[0:2])
			},
		},
		{
			name: "list non-first page",
			request: &operatorservice.ListNexusIncomingServicesRequest{
				NextPageToken: nextPageToken,
				PageSize:      2,
			},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.ProtoEqual(resp.Services[0], servicesOrdered[2])
			},
		},
		{
			name:    "list with no page size",
			request: &operatorservice.ListNexusIncomingServicesRequest{},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.NotEmpty(resp.Services)
			},
		},
		{
			name: "list with filter found",
			request: &operatorservice.ListNexusIncomingServicesRequest{
				NextPageToken: nil,
				PageSize:      2,
				Name:          serviceToFilter.Spec.Name,
			},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.Nil(resp.NextPageToken)
				s.Len(resp.Services, 1)
				s.ProtoEqual(resp.Services[0], serviceToFilter)
			},
		},
		{
			name: "list with filter not found",
			request: &operatorservice.ListNexusIncomingServicesRequest{
				NextPageToken: nil,
				PageSize:      2,
				Name:          "missing-service",
			},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.Nil(resp.NextPageToken)
				s.Empty(resp.Services)
			},
		},
		{
			name: "list with malformed filter",
			request: &operatorservice.ListNexusIncomingServicesRequest{
				NextPageToken: nextPageToken,
				PageSize:      2,
				Name:          "\n```\n",
			},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
		{
			name: "list with page size too large",
			request: &operatorservice.ListNexusIncomingServicesRequest{
				NextPageToken: nil,
				PageSize:      1005,
			},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.ListNexusIncomingServices(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestGetNexusIncomingService_Operator() {
	service := s.createNexusIncomingService(s.T().Name())

	type testcase struct {
		name      string
		request   *operatorservice.GetNexusIncomingServiceRequest
		assertion func(*operatorservice.GetNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid get",
			request: &operatorservice.GetNexusIncomingServiceRequest{
				Id: service.Id,
			},
			assertion: func(response *operatorservice.GetNexusIncomingServiceResponse, err error) {
				s.NoError(err)
				s.ProtoEqual(service, response.Service)
			},
		},
		{
			name: "invalid: missing service",
			request: &operatorservice.GetNexusIncomingServiceRequest{
				Id: uuid.NewString(),
			},
			assertion: func(response *operatorservice.GetNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:    "invalid: service ID not set",
			request: &operatorservice.GetNexusIncomingServiceRequest{},
			assertion: func(response *operatorservice.GetNexusIncomingServiceResponse, err error) {
				s.ErrorContains(err, "incoming service Id not set")
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.GetNexusIncomingService(NewContext(), tc.request)
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestListNexusIncomingServicesOrdering() {
	// get initial table version since it has been modified by other tests
	resp, err := s.testCluster.GetMatchingClient().ListNexusIncomingServices(NewContext(), &matchingservice.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: 0,
		PageSize:              0,
	})
	s.NoError(err)
	initialTableVersion := resp.TableVersion

	// create some services
	numServices := 40 // minimum number of services to test, there may be more in DB from other tests
	for i := 0; i < numServices; i++ {
		s.createNexusIncomingService(s.randomizeStr("test-service-name"))
	}
	tableVersion := initialTableVersion + int64(numServices)

	// list from persistence manager level
	persistence := s.testCluster.testBase.NexusIncomingServiceManager
	persistenceResp1, err := persistence.ListNexusIncomingServices(NewContext(), &p.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numServices / 2,
	})
	s.NoError(err)
	s.Len(persistenceResp1.Entries, numServices/2)
	s.NotNil(persistenceResp1.NextPageToken)
	persistenceResp2, err := persistence.ListNexusIncomingServices(NewContext(), &p.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              numServices / 2,
		NextPageToken:         persistenceResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(persistenceResp2.Entries, numServices/2)

	// list from matching level
	matchingClient := s.testCluster.GetMatchingClient()
	matchingResp1, err := matchingClient.ListNexusIncomingServices(NewContext(), &matchingservice.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numServices / 2),
	})
	s.NoError(err)
	s.Len(matchingResp1.Services, numServices/2)
	s.NotNil(matchingResp1.NextPageToken)
	matchingResp2, err := matchingClient.ListNexusIncomingServices(NewContext(), &matchingservice.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numServices / 2),
		NextPageToken:         matchingResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(matchingResp2.Services, numServices/2)

	// list from operator level
	operatorResp1, err := s.operatorClient.ListNexusIncomingServices(NewContext(), &operatorservice.ListNexusIncomingServicesRequest{
		PageSize: int32(numServices / 2),
	})
	s.NoError(err)
	s.Len(operatorResp1.Services, numServices/2)
	s.NotNil(operatorResp1.NextPageToken)
	operatorResp2, err := s.operatorClient.ListNexusIncomingServices(NewContext(), &operatorservice.ListNexusIncomingServicesRequest{
		PageSize:      int32(numServices / 2),
		NextPageToken: operatorResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(operatorResp2.Services, numServices/2)

	// assert list orders match
	for i := 0; i < numServices/2; i++ {
		s.Equal(persistenceResp1.Entries[i].Id, matchingResp1.Services[i].Id)
		s.Equal(persistenceResp2.Entries[i].Id, matchingResp2.Services[i].Id)

		s.Equal(persistenceResp1.Entries[i].Id, operatorResp1.Services[i].Id)
		s.Equal(persistenceResp2.Entries[i].Id, operatorResp2.Services[i].Id)
	}
}

func (s *FunctionalSuite) createNexusIncomingService(name string) *nexus.IncomingService {
	resp, err := s.testCluster.GetMatchingClient().CreateNexusIncomingService(
		NewContext(),
		&matchingservice.CreateNexusIncomingServiceRequest{
			Spec: &nexus.IncomingServiceSpec{
				Name:      name,
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
		})

	s.NoError(err)
	s.NotNil(resp.Service)
	return resp.Service
}
