package archiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
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
