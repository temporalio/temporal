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
	"cmp"
	"slices"
	"testing"

	"go.temporal.io/api/nexus/v1"
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
				Name:      "testService",
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
				Name:      "testService",
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
				Name:      "testService",
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
				var invalidArgErr *serviceerror.InvalidArgument
				s.ErrorAs(err, &invalidArgErr)
			},
		},
		{
			name: "invalid update: service version mismatch",
			service: &nexus.IncomingService{
				Version:   1,
				Name:      "testService",
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
			serviceName: "service-to-delete",
			assertion: func(resp *matchingservice.DeleteNexusIncomingServiceResponse, err error) {
				s.NoError(err)
			},
		},
	}

	s.createNexusIncomingService("service-to-delete")
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
	s0 := s.createNexusIncomingService("list-test-service0")
	s1 := s.createNexusIncomingService("list-test-service1")
	s2 := s.createNexusIncomingService("list-test-service2")
	servicesOrdered := []*persistencepb.NexusIncomingServiceEntry{s0, s1, s2}
	slices.SortFunc(servicesOrdered, func(a *persistencepb.NexusIncomingServiceEntry, b *persistencepb.NexusIncomingServiceEntry) int {
		return cmp.Compare(a.Id, b.Id)
	})

	// get expected table version for the course of the tests
	matchingClient := s.testCluster.GetMatchingClient()
	resp, err := matchingClient.ListNexusIncomingServices(
		NewContext(),
		&matchingservice.ListNexusIncomingServicesRequest{
			PageSize:              0,
			LastKnownTableVersion: 0,
			Wait:                  false,
		})
	s.NoError(err)
	s.NotNil(resp)
	tableVersion := resp.TableVersion
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
				s.Nil(resp.NextPageToken)
				s.ProtoElementsMatch(resp.Entries, servicesOrdered)
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
				s.Nil(resp.NextPageToken)
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
				s.createNexusIncomingService("new-service")
			}
			<-listReqDone
		})
	}
}

func (s *FunctionalSuite) TestListNexusIncomingServicesOrdering_Matching() {
	// create some services
	numServices := 40
	for i := 0; i < numServices; i++ {
		s.createNexusIncomingService(s.randomizeStr("test-service-name"))
	}

	// list from persistence manager level
	persistence := s.testCluster.testBase.NexusIncomingServiceManager
	persistenceResp1, err := persistence.ListNexusIncomingServices(NewContext(), &p.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: int64(numServices),
		PageSize:              numServices / 2,
	})
	s.NoError(err)
	s.Len(persistenceResp1.Entries, numServices/2)
	s.NotNil(persistenceResp1.NextPageToken)
	persistenceResp2, err := persistence.ListNexusIncomingServices(NewContext(), &p.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: int64(numServices),
		PageSize:              (numServices / 2) + 1,
		NextPageToken:         persistenceResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(persistenceResp2.Entries, numServices/2)
	s.Nil(persistenceResp2.NextPageToken)

	// list from matching level
	matchingClient := s.testCluster.GetMatchingClient()
	matchingResp1, err := matchingClient.ListNexusIncomingServices(NewContext(), &matchingservice.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: int64(numServices),
		PageSize:              int32(numServices / 2),
	})
	s.NoError(err)
	s.Len(matchingResp1.Entries, numServices/2)
	s.NotNil(matchingResp1.NextPageToken)
	matchingResp2, err := matchingClient.ListNexusIncomingServices(NewContext(), &matchingservice.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: int64(numServices),
		PageSize:              int32(numServices/2) + 1,
		NextPageToken:         matchingResp1.NextPageToken,
	})
	s.NoError(err)
	s.Len(matchingResp2.Entries, numServices/2)
	s.Nil(matchingResp2.NextPageToken)

	// assert list orders match
	for i := 0; i < numServices/2; i++ {
		s.ProtoEqual(persistenceResp1.Entries[i], matchingResp1.Entries[i])
		s.ProtoEqual(persistenceResp2.Entries[i], matchingResp2.Entries[i])
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
