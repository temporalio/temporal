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

package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
)

type (
	metadataSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		logger                     log.Logger
		timeSource                 *clock.EventTimeSource
		mockClusterMetadataManager *persistence.MockClusterMetadataManager
		metadata                   Metadata
	}
)

func TestNamespaceCacheSuite(t *testing.T) {
	suite.Run(t, &metadataSuite{})
}

func (s *metadataSuite) SetupSuite() {
}

func (s *metadataSuite) TearDownSuite() {

}

func (s *metadataSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.logger = log.NewDefaultLogger()
	s.timeSource = clock.NewEventTimeSource()
	s.mockClusterMetadataManager = persistence.NewMockClusterMetadataManager(s.controller)
	s.metadata = NewTestClusterMetadata(NewTestClusterMetadataConfig(false, true), s.mockClusterMetadataManager, s.timeSource)
}

func (s *metadataSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *metadataSuite) TestGetSearchAttributesCache() {
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 0, 0, time.UTC))
	// Initial call
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata().Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)
	// Second call, no changes in DB (version is the same)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata().Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)
	// Third call, DB changed
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata().Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 2,
	}, nil)

	wg := sync.WaitGroup{}
	wg.Add(10)
	for goroutine := 0; goroutine < 10; goroutine++ {
		go func(goroutine int) {
			for i := 1; i < 1500; i++ {
				searchAttributes, err := s.metadata.GetSearchAttributes("index-name", false)
				s.NoError(err)
				searchAttributes = searchattribute.FilterCustom(searchAttributes)
				s.Len(searchAttributes, 1)
				s.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, searchAttributes["OrderId"])
				if i%500 == 0 && goroutine == 5 {
					// This moves time two times.
					s.timeSource.Update(s.timeSource.Now().Add(searchAttributeCacheRefreshInterval).Add(time.Second))
				}
			}
			wg.Done()
		}(goroutine)
	}
	wg.Wait()
}

func (s *metadataSuite) TestSaveSearchAttributes_UpdateIndex() {
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata().Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderIdOld": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)

	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(&persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}).Return(false, nil)

	err := s.metadata.SaveSearchAttributes("index-name", map[string]enumspb.IndexedValueType{
		"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	s.NoError(err)
}
func (s *metadataSuite) TestSaveSearchAttributes_NewIndex() {
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata().Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name-2": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)

	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(&persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name-2": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}},
				"index-name": {
					SearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}).Return(false, nil)

	err := s.metadata.SaveSearchAttributes("index-name", map[string]enumspb.IndexedValueType{
		"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	s.NoError(err)
}
