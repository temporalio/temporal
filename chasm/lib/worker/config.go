package worker

import (
	"go.temporal.io/server/common/dynamicconfig"
)

type Config struct {
	// Future configs can be added here.
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{}
}
