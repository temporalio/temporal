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

package searchattribute

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type (
	searchAttributesManagerSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller

		logger                     log.Logger
		timeSource                 *clock.EventTimeSource
		mockClusterMetadataManager *persistence.MockClusterMetadataManager
		manager                    *managerImpl
		forceCacheRefresh          bool
	}
)

func TestSearchAttributesManagerSuite(t *testing.T) {
	suite.Run(t, &searchAttributesManagerSuite{})
}

func (s *searchAttributesManagerSuite) SetupSuite() {
}

func (s *searchAttributesManagerSuite) TearDownSuite() {

}

func (s *searchAttributesManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
	s.timeSource = clock.NewEventTimeSource()
	s.mockClusterMetadataManager = persistence.NewMockClusterMetadataManager(s.controller)
	s.manager = NewManager(
		s.timeSource,
		s.mockClusterMetadataManager,
		func(opts ...dynamicconfig.FilterOption) bool {
			return s.forceCacheRefresh
		},
	)
}

func (s *searchAttributesManagerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *searchAttributesManagerSuite) TestGetSearchAttributesCache() {
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 0, 0, time.UTC))
	// Initial call
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)
	// Second call, no changes in DB (version is the same)
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 10, 0, time.UTC))
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)
	// Third call, DB changed
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 20, 0, time.UTC))
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 2,
	}, nil)

	wg := sync.WaitGroup{}
	wg.Add(10)
	for goroutine := 0; goroutine < 10; goroutine++ {
		go func(goroutine int) {
			defer wg.Done()
			for i := 1; i < 1500; i++ {
				searchAttributes, err := s.manager.GetSearchAttributes("index-name", false)
				s.NoError(err)
				s.Len(searchAttributes.Custom(), 1)
				t, err := searchAttributes.GetType("OrderId")
				s.NoError(err)
				s.Equal(enumspb.INDEXED_VALUE_TYPE_KEYWORD, t)
				if i%500 == 0 && goroutine == 5 {
					// This moves time two times.
					s.timeSource.Update(s.timeSource.Now().Add(cacheRefreshInterval).Add(time.Second))
				}
			}
		}(goroutine)
	}
	wg.Wait()
}

func (s *searchAttributesManagerSuite) TestGetSearchAttributesCache_Error() {
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 0, 0, time.UTC))
	// Initial call
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, errors.New("random error"))
	searchAttributes, err := s.manager.GetSearchAttributes("index-name", false)
	s.Error(err)
	s.Len(searchAttributes.Custom(), 0)
}

func (s *searchAttributesManagerSuite) TestGetSearchAttributesCache_NotFoundError() {
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 0, 0, time.UTC))

	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, serviceerror.NewNotFound("not found"))
	searchAttributes, err := s.manager.GetSearchAttributes("index-name", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 0)

	// GetClusterMetadata() shouldn't be called, because results are cached.
	searchAttributes, err = s.manager.GetSearchAttributes("index-name", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 0)
}

func (s *searchAttributesManagerSuite) TestGetSearchAttributesCache_UnavailableError() {
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 0, 0, time.UTC))

	// First call populates cache.
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)
	searchAttributes, err := s.manager.GetSearchAttributes("index-name", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 1)

	// Expire cache.
	s.timeSource.Update(time.Date(2020, 8, 22, 2, 0, 0, 0, time.UTC))

	// Second call, cache is expired, DB is down, but cache data is returned.
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(nil, serviceerror.NewUnavailable("db is down"))
	searchAttributes, err = s.manager.GetSearchAttributes("index-name", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 1)

	// Next cache refresh in cacheRefreshIfUnavailableInterval.
	c := s.manager.cache.Load().(cache)
	s.Equal(time.Date(2020, 8, 22, 2, 0, 20, 0, time.UTC), c.expireOn)
}

func (s *searchAttributesManagerSuite) TestGetSearchAttributesCache_EmptyIndex() {
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)

	searchAttributes, err := s.manager.GetSearchAttributes("", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 1)
}

func (s *searchAttributesManagerSuite) TestGetSearchAttributesCache_RefreshIfAbsent() {
	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 0, 0, time.UTC))

	// First call populates cache.
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{},
		},
		Version: 1,
	}, nil)

	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 2,
	}, nil)

	searchAttributes, err := s.manager.GetSearchAttributes("index-name", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 0)

	s.timeSource.Update(time.Date(2020, 8, 22, 1, 0, 1, 0, time.UTC))

	searchAttributes, err = s.manager.GetSearchAttributes("index-name", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 0)

	s.forceCacheRefresh = true
	searchAttributes, err = s.manager.GetSearchAttributes("index-name", false)
	s.NoError(err)
	s.Len(searchAttributes.Custom(), 1)
}

func (s *searchAttributesManagerSuite) TestSaveSearchAttributes_UpdateIndex() {
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderIdOld": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)

	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}).Return(false, nil)

	err := s.manager.SaveSearchAttributes(context.Background(), "index-name", map[string]enumspb.IndexedValueType{
		"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	s.NoError(err)
}
func (s *searchAttributesManagerSuite) TestSaveSearchAttributes_NewIndex() {
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name-2": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)

	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name-2": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}},
				"index-name": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}).Return(false, nil)

	err := s.manager.SaveSearchAttributes(context.Background(), "index-name", map[string]enumspb.IndexedValueType{
		"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	s.NoError(err)
}

func (s *searchAttributesManagerSuite) TestSaveSearchAttributesCache_EmptyIndex() {
	s.mockClusterMetadataManager.EXPECT().GetCurrentClusterMetadata(gomock.Any()).Return(&persistence.GetClusterMetadataResponse{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name-2": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}, nil)

	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			IndexSearchAttributes: map[string]*persistencespb.IndexSearchAttributes{
				"index-name-2": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}},
				"": {
					CustomSearchAttributes: map[string]enumspb.IndexedValueType{
						"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					}}},
		},
		Version: 1,
	}).Return(false, nil)

	err := s.manager.SaveSearchAttributes(context.Background(), "", map[string]enumspb.IndexedValueType{
		"OrderId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	s.NoError(err)
}
