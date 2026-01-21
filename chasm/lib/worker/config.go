package worker

import (
	"go.temporal.io/server/common/dynamicconfig"
)

type Config struct {
	EnableActivityRescheduling dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		EnableActivityRescheduling: dynamicconfig.EnableWorkerActivityRescheduling.Get(dc),
	}
}
