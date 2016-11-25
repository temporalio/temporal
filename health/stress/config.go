package stress

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
)

// Configuration is the configuration used by cherami-stress
type Configuration struct {
	Logging      log.Configuration
	StressConfig WorkflowStressConfiguration `yaml:"stress"`
}

// WorkflowStressConfiguration encompasses stress configuration
type WorkflowStressConfiguration struct {
	Metrics        metrics.Configuration `yaml:"metrics"`
	WorkflowConfig WorkflowConfiguration `yaml:"workflow"`
}

// WorkflowConfiguration is the configuration of number of workflows to launch.
type WorkflowConfiguration struct {
	TotalLaunchCount int `yaml:"totalLaunchCount"`
	RoutineCount     int `yaml:"routineCount"`
	ChainSequence    int `yaml:"chainSequence"`
	ActivitySleepMin int `yaml:"activitySleepMin"`
	ActivitySleepMax int `yaml:"activitySleepMax"`
}
