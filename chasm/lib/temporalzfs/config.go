package temporalzfs

import (
	"time"

	temporalzfspb "go.temporal.io/server/chasm/lib/temporalzfs/gen/temporalzfspb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	Enabled = dynamicconfig.NewNamespaceBoolSetting(
		"temporalzfs.enabled",
		false,
		`Toggles TemporalZFS functionality on the server.`,
	)
)

const (
	defaultChunkSize            = 256 * 1024 // 256KB
	defaultMaxSize              = 1 << 30    // 1GB
	defaultMaxFiles             = 100_000
	defaultGCInterval           = 5 * time.Minute
	defaultSnapshotRetention    = 24 * time.Hour
	defaultOwnerCheckInterval   = 10 * time.Minute
	ownerCheckNotFoundThreshold = int32(2)
	dataCleanupMaxBackoff       = 30 * time.Minute
)

type Config struct {
	Enabled dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		Enabled: Enabled.Get(dc),
	}
}

func defaultConfig() *temporalzfspb.FilesystemConfig {
	return &temporalzfspb.FilesystemConfig{
		ChunkSize:         defaultChunkSize,
		MaxSize:           defaultMaxSize,
		MaxFiles:          defaultMaxFiles,
		GcInterval:        durationpb.New(defaultGCInterval),
		SnapshotRetention: durationpb.New(defaultSnapshotRetention),
	}
}
