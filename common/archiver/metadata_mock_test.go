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

package archiver

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestMetadataMock(t *testing.T) {
	t.Run("GetHistoryConfig", func(t *testing.T) {
		metadata := NewMetadataMock(gomock.NewController(t))
		config := metadata.GetHistoryConfig()

		assert.False(t, config.ClusterConfiguredForArchival())
	})
	t.Run("GetVisibilityConfig", func(t *testing.T) {
		metadata := NewMetadataMock(gomock.NewController(t))
		config := metadata.GetVisibilityConfig()

		assert.False(t, config.ClusterConfiguredForArchival())
	})
	t.Run("GetHistoryConfig_SetHistoryEnabledByDefault", func(t *testing.T) {
		metadata := NewMetadataMock(gomock.NewController(t))
		metadata.SetHistoryEnabledByDefault()
		config := metadata.GetHistoryConfig()

		assert.True(t, config.ClusterConfiguredForArchival())

		metadata.EXPECT().GetHistoryConfig().Return(NewDisabledArchvialConfig())
		config = metadata.GetHistoryConfig()

		assert.False(t, config.ClusterConfiguredForArchival())
	})
	t.Run("GetVisibilityConfig_SetVisibilityEnabledByDefault", func(t *testing.T) {
		metadata := NewMetadataMock(gomock.NewController(t))
		metadata.SetVisibilityEnabledByDefault()
		config := metadata.GetVisibilityConfig()

		assert.True(t, config.ClusterConfiguredForArchival())

		metadata.EXPECT().GetVisibilityConfig().Return(NewDisabledArchvialConfig())
		config = metadata.GetVisibilityConfig()

		assert.False(t, config.ClusterConfiguredForArchival())
	})
	t.Run("EXPECT_GetHistoryConfig", func(t *testing.T) {
		metadata := NewMetadataMock(gomock.NewController(t))
		metadata.EXPECT().GetHistoryConfig().Return(NewEnabledArchivalConfig())
		config := metadata.GetHistoryConfig()

		assert.True(t, config.ClusterConfiguredForArchival())

		metadata.EXPECT().GetHistoryConfig().Return(NewDisabledArchvialConfig())
		config = metadata.GetHistoryConfig()

		assert.False(t, config.ClusterConfiguredForArchival())
	})

	t.Run("EXPECT_GetVisibilityConfig", func(t *testing.T) {
		metadata := NewMetadataMock(gomock.NewController(t))
		metadata.EXPECT().GetVisibilityConfig().Return(NewEnabledArchivalConfig())
		config := metadata.GetVisibilityConfig()

		assert.True(t, config.ClusterConfiguredForArchival())

		metadata.EXPECT().GetVisibilityConfig().Return(NewDisabledArchvialConfig())
		config = metadata.GetVisibilityConfig()

		assert.False(t, config.ClusterConfiguredForArchival())
	})
}
