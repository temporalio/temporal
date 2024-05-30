package scheduler

import (
	"go.temporal.io/server/common/dynamicconfig"
)

type Config struct {
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{}
}