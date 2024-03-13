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

package tests

import (
	"context"
	"strconv"

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"
)

// This file contains tests for the Nexus outgoing service registry. It verifies basic CRUD operations from the operator
// API.

func (s *FunctionalSuite) TestOutgoingServiceRegistry() {
	// Use a unique namespace to avoid conditional update conflicts due to concurrent writes.
	ns := s.randomizeStr("outgoing-service-registry-test")
	s.NoError(s.registerNamespaceWithDefaults(ns))

	ctx := context.Background()

	testURL := "http://localhost/"
	s.Run("UpdateNonExistentService", func() {
		serviceName := s.randomizeStr("service-name")
		_, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
			Version:   1,
			Namespace: ns,
			Name:      serviceName,
			Url:       testURL,
		})
		s.Error(err)
		s.Assert().Equal(codes.NotFound, serviceerror.ToStatus(err).Code(),
			"should return not found error when trying to update non-existent service")
	})

	s.Run("CreateAndGet", func() {
		serviceName := s.randomizeStr("service-name")
		{
			response, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Version:   0,
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL,
			})
			s.NoError(err)
			s.NotNil(response)
		}
		{
			response, err := s.operatorClient.GetNexusOutgoingService(ctx, &operatorservice.GetNexusOutgoingServiceRequest{
				Namespace: ns,
				Name:      serviceName,
			})
			s.NoError(err)
			s.Assert().Equal(1, int(response.Service.Version))
			s.Assert().Equal(serviceName, response.Service.Name)
			s.Assert().Equal(testURL, response.Service.Url)
		}
	})

	s.Run("CreateAndUpdateWrongVersion", func() {
		serviceName := s.randomizeStr("service-name")
		{
			response, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL,
			})
			s.NoError(err)
			s.NotNil(response)
			s.Assert().Equal(1, int(response.Service.Version))
		}
		{
			_, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Version:   2,
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL,
			})
			s.Error(err)
			s.Assert().Equal(codes.FailedPrecondition, serviceerror.ToStatus(err).Code(),
				"should return failed precondition error when trying to update with wrong version")
		}
	})

	s.Run("CreateAndUpdateZeroVersion", func() {
		serviceName := s.randomizeStr("service-name")
		{
			response, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL,
			})
			s.NoError(err)
			s.NotNil(response)
			s.Assert().Equal(1, int(response.Service.Version))
		}
		{
			_, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Version:   0,
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL,
			})
			s.Error(err)
			s.Assert().Equal(codes.AlreadyExists, serviceerror.ToStatus(err).Code(),
				"should return already exists error when trying to update with zero version (requesting creation)")
		}
	})

	s.Run("CreateAndUpdateCorrectVersion", func() {
		serviceName := s.randomizeStr("service-name")
		{
			response, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL,
			})
			s.NoError(err)
			s.NotNil(response)
			s.Assert().Equal(1, int(response.Service.Version))
		}
		{
			response, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Version:   1,
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL + "x",
			})
			s.NoError(err)
			s.Assert().Equal(2, int(response.Service.Version))
			s.Assert().Equal(testURL+"x", response.Service.Url)
		}
		{
			response, err := s.operatorClient.GetNexusOutgoingService(ctx, &operatorservice.GetNexusOutgoingServiceRequest{
				Namespace: ns,
				Name:      serviceName,
			})
			s.NoError(err)
			s.Assert().Equal(2, int(response.Service.Version))
			s.Assert().Equal(serviceName, response.Service.Name)
		}
	})

	s.Run("CreateAndDelete", func() {
		serviceName := s.randomizeStr("service-name")
		{
			response, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Version:   0,
				Namespace: ns,
				Name:      serviceName,
				Url:       testURL,
			})
			s.NoError(err)
			s.NotNil(response)
		}
		{
			_, err := s.operatorClient.DeleteNexusOutgoingService(ctx, &operatorservice.DeleteNexusOutgoingServiceRequest{
				Namespace: ns,
				Name:      serviceName,
			})
			s.NoError(err)
		}
		{
			_, err := s.operatorClient.GetNexusOutgoingService(ctx, &operatorservice.GetNexusOutgoingServiceRequest{
				Namespace: ns,
				Name:      serviceName,
			})
			s.Error(err)
			s.Assert().Equal(codes.NotFound, serviceerror.ToStatus(err).Code(), err)
		}
	})

	s.Run("CreateAndList", func() {
		// Make another unique namespace to avoid listing services from previous tests.
		ns := s.randomizeStr("list-nexus-outgoing-services-test")
		s.NoError(s.registerNamespaceWithDefaults(ns))

		serviceName := s.randomizeStr("service-name")
		for i := 0; i < 10; i++ {
			response, err := s.operatorClient.CreateOrUpdateNexusOutgoingService(ctx, &operatorservice.CreateOrUpdateNexusOutgoingServiceRequest{
				Version:   0,
				Namespace: ns,
				Name:      serviceName + strconv.Itoa(i),
				Url:       testURL,
			})
			s.NoError(err)
			s.NotNil(response)
		}
		var nextPageToken []byte
		services := make([]*nexus.OutgoingService, 0, 10)
		for {
			response, err := s.operatorClient.ListNexusOutgoingServices(ctx, &operatorservice.ListNexusOutgoingServicesRequest{
				Namespace:     ns,
				NextPageToken: nextPageToken,
			})
			s.NoError(err)
			services = append(services, response.Services...)
			nextPageToken = response.NextPageToken
			if len(nextPageToken) == 0 {
				break
			}
		}
		s.Assert().Len(services, 10)
		for i := 0; i < 10; i++ {
			s.Assert().Equal(serviceName+strconv.Itoa(i), services[i].Name)
		}
	})
}
