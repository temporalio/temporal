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
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
)

func (s *FunctionalSuite) TestCreateOrUpdateNexusIncomingService_Matching() {
	type testcase struct {
		name      string
		service   *nexus.IncomingService
		assertion func(*matchingservice.CreateOrUpdateNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid create",
			service: &nexus.IncomingService{
				Version:   0,
				Name:      "matchingTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *matchingservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Entry)
				s.Equal(int64(1), resp.Entry.Version)
			},
		},
		{
			name: "invalid create: service already exists",
			service: &nexus.IncomingService{
				Version:   0,
				Name:      "matchingTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *matchingservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				var existsErr *serviceerror.AlreadyExists
				s.ErrorAs(err, &existsErr)
			},
		},
		{
			name: "valid update",
			service: &nexus.IncomingService{
				Version:   1,
				Name:      "matchingTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *matchingservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Entry)
				s.Equal(int64(2), resp.Entry.Version)
			},
		},
		{
			name: "invalid update: service not found",
			service: &nexus.IncomingService{
				Version:   1,
				Name:      "missing-service",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *matchingservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid update: service version mismatch",
			service: &nexus.IncomingService{
				Version:   1,
				Name:      "matchingTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *matchingservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
	}

	matchingClient := s.testCluster.GetMatchingClient()
	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.CreateOrUpdateNexusIncomingService(
				NewContext(),
				&matchingservice.CreateOrUpdateNexusIncomingServiceRequest{
					Service: tc.service,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestDeleteNexusIncomingService_Matching() {
	type testcase struct {
		name        string
		serviceName string
		assertion   func(*matchingservice.DeleteNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name:        "invalid delete: not found",
			serviceName: "missing-service",
			assertion: func(resp *matchingservice.DeleteNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:        "valid delete",
			serviceName: "matching-service-to-delete",
			assertion: func(resp *matchingservice.DeleteNexusIncomingServiceResponse, err error) {
				s.NoError(err)
			},
		},
	}

	s.createNexusIncomingService("matching-service-to-delete")
	matchingClient := s.testCluster.GetMatchingClient()
	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := matchingClient.DeleteNexusIncomingService(
				NewContext(),
				&matchingservice.DeleteNexusIncomingServiceRequest{
					Name: tc.serviceName,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestListNexusIncomingServices_Matching() {
	// initialize some services
	s.createNexusIncomingService("matching-list-test-service0")
	s.createNexusIncomingService("matching-list-test-service1")
	s.createNexusIncomingService("matching-list-test-service2")

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
	servicesOrdered := resp.Entries
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
				s.Equal(nextPageToken, resp.NextPageToken)
				s.ProtoElementsMatch(resp.Entries, servicesOrdered[0:2])
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
				s.ProtoElementsMatch(resp.Entries, servicesOrdered[0:3])
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
				s.ProtoEqual(resp.Entries[0], servicesOrdered[2])
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
				s.Len(resp.Entries, 3)
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
				s.createNexusIncomingService("matching-new-service")
			}
			<-listReqDone
		})
	}
}

func (s *FunctionalSuite) TestCreateOrUpdateNexusIncomingService_Operator() {
	type testcase struct {
		name      string
		service   *nexus.IncomingService
		assertion func(*operatorservice.CreateOrUpdateNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name: "valid create",
			service: &nexus.IncomingService{
				Version:   0,
				Name:      "operatorTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *operatorservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Service)
				s.Equal(int64(1), resp.Service.Version)
			},
		},
		{
			name: "invalid create: service already exists",
			service: &nexus.IncomingService{
				Version:   0,
				Name:      "operatorTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *operatorservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				var existsErr *serviceerror.AlreadyExists
				s.ErrorAs(err, &existsErr)
			},
		},
		{
			name: "valid update",
			service: &nexus.IncomingService{
				Version:   1,
				Name:      "operatorTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *operatorservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				s.NoError(err)
				s.NotNil(resp.Service)
				s.Equal(int64(2), resp.Service.Version)
			},
		},
		{
			name: "invalid update: service not found",
			service: &nexus.IncomingService{
				Version:   1,
				Name:      "missing-service",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *operatorservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name: "invalid update: service version mismatch",
			service: &nexus.IncomingService{
				Version:   1,
				Name:      "operatorTestService",
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
			assertion: func(resp *operatorservice.CreateOrUpdateNexusIncomingServiceResponse, err error) {
				var invalidErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidErr)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.CreateOrUpdateNexusIncomingService(
				NewContext(),
				&operatorservice.CreateOrUpdateNexusIncomingServiceRequest{
					Service: tc.service,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestDeleteNexusIncomingService_Operator() {
	type testcase struct {
		name        string
		serviceName string
		assertion   func(*operatorservice.DeleteNexusIncomingServiceResponse, error)
	}
	testCases := []testcase{
		{
			name:        "invalid delete: not found",
			serviceName: "missing-service",
			assertion: func(resp *operatorservice.DeleteNexusIncomingServiceResponse, err error) {
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
			},
		},
		{
			name:        "valid delete",
			serviceName: "operator-service-to-delete",
			assertion: func(resp *operatorservice.DeleteNexusIncomingServiceResponse, err error) {
				s.NoError(err)
			},
		},
	}

	s.createNexusIncomingService("operator-service-to-delete")
	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			resp, err := s.operatorClient.DeleteNexusIncomingService(
				NewContext(),
				&operatorservice.DeleteNexusIncomingServiceRequest{
					Name: tc.serviceName,
				})
			tc.assertion(resp, err)
		})
	}
}

func (s *FunctionalSuite) TestListNexusIncomingServices_Operator() {
	// initialize some services
	s.createNexusIncomingService("operator-list-test-service0")
	s.createNexusIncomingService("operator-list-test-service1")
	s.createNexusIncomingService("operator-list-test-service2")

	// get expected table version and services for the course of the tests
	resp, err := s.operatorClient.ListNexusIncomingServices(
		NewContext(),
		&operatorservice.ListNexusIncomingServicesRequest{
			PageSize: 100,
		})
	s.NoError(err)
	s.NotNil(resp)
	servicesOrdered := resp.Services

	matchingResp, err := s.testCluster.GetMatchingClient().ListNexusIncomingServices(NewContext(), &matchingservice.ListNexusIncomingServicesRequest{PageSize: 2})
	s.NoError(err)
	s.NotNil(matchingResp)
	s.NotNil(matchingResp.NextPageToken)
	nextPageToken := matchingResp.NextPageToken

	type testcase struct {
		name      string
		request   *operatorservice.ListNexusIncomingServicesRequest
		assertion func(*operatorservice.ListNexusIncomingServicesResponse, error)
	}
	testCases := []testcase{
		{
			name: "list nexus incoming services: first_page=true",
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
			name: "list nexus incoming services: first_page=false",
			request: &operatorservice.ListNexusIncomingServicesRequest{
				NextPageToken: nextPageToken,
				PageSize:      2,
			},
			assertion: func(resp *operatorservice.ListNexusIncomingServicesResponse, err error) {
				s.NoError(err)
				s.ProtoEqual(resp.Services[0], servicesOrdered[2])
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
	s.Len(matchingResp1.Entries, numServices/2)
	s.NotNil(matchingResp1.NextPageToken)
	matchingResp2, err := matchingClient.ListNexusIncomingServices(NewContext(), &matchingservice.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: tableVersion,
		PageSize:              int32(numServices / 2),
		NextPageToken:         matchingResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(matchingResp2.Entries, numServices/2)

	// list from frontend operator service level
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

	for i := 0; i < numServices/2; i++ {
		// assert matching order is the same as persisted
		s.ProtoEqual(persistenceResp1.Entries[i], matchingResp1.Entries[i])
		s.ProtoEqual(persistenceResp2.Entries[i], matchingResp2.Entries[i])

		// assert operator order is the same as persisted
		s.ProtoEqual(entryToService(persistenceResp1.Entries[i]), operatorResp1.Services[i])
		s.ProtoEqual(entryToService(persistenceResp2.Entries[i]), operatorResp2.Services[i])
	}
}

func (s *FunctionalSuite) createNexusIncomingService(name string) *persistencepb.NexusIncomingServiceEntry {
	resp, err := s.testCluster.GetMatchingClient().CreateOrUpdateNexusIncomingService(
		NewContext(),
		&matchingservice.CreateOrUpdateNexusIncomingServiceRequest{
			Service: &nexus.IncomingService{
				Version:   0,
				Name:      name,
				Namespace: s.namespace,
				TaskQueue: s.defaultTaskQueue().Name,
			},
		})

	s.NoError(err)
	s.NotNil(resp.Entry)
	return resp.Entry
}

func entryToService(entry *persistencepb.NexusIncomingServiceEntry) *nexus.IncomingService {
	return &nexus.IncomingService{
		Version:   entry.Version,
		Name:      entry.Service.Name,
		Namespace: entry.Service.NamespaceId,
		TaskQueue: entry.Service.TaskQueue,
		Metadata:  entry.Service.Metadata,
	}
}
