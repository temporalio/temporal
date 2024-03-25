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

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"

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
	service := s.createNexusIncomingService("service-to-delete")
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

func (s *FunctionalSuite) TestListNexusIncomingServicesOrdering_Matching() {
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

	// assert list orders match
	for i := 0; i < numServices/2; i++ {
		s.Equal(persistenceResp1.Entries[i].Id, matchingResp1.Services[i].Id)
		s.Equal(persistenceResp2.Entries[i].Id, matchingResp2.Services[i].Id)
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
