package temporalfs

import (
	"time"

	temporalfspb "go.temporal.io/server/chasm/lib/temporalfs/gen/temporalfspb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	Enabled = dynamicconfig.NewNamespaceBoolSetting(
		"temporalfs.enabled",
		false,
		`Toggles TemporalFS functionality on the server.`,
	)
)

const (
	defaultChunkSize         = 256 * 1024 // 256KB
	defaultMaxSize           = 1 << 30    // 1GB
	defaultMaxFiles          = 100_000
	defaultGCInterval        = 5 * time.Minute
	defaultSnapshotRetention = 24 * time.Hour
)

type Config struct {
	Enabled dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled: Enabled.Get(dc),
	}
}

func defaultConfig() *temporalfspb.FilesystemConfig {
	return &temporalfspb.FilesystemConfig{
		ChunkSize:         defaultChunkSize,
		MaxSize:           defaultMaxSize,
		MaxFiles:          defaultMaxFiles,
		GcInterval:        durationpb.New(defaultGCInterval),
		SnapshotRetention: durationpb.New(defaultSnapshotRetention),
	}
}
