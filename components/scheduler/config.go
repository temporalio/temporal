package scheduler

import (
	"go.temporal.io/server/common/dynamicconfig"
)

var UseExperimentalHsmScheduler = dynamicconfig.NewNamespaceBoolSetting(
	"scheduler.use-experimental-hsm-scheduler",
	false,
	"When true, use the experimental scheduler implemented using the HSM framework instead of workflows")

type Config struct {
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{}
}
