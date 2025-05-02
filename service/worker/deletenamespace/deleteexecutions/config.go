package deleteexecutions

import (
	"encoding/json"
)

const (
	// Deprecated.
	// TODO: Remove after 1.27 release.
	defaultDeleteActivityRPS                    = 100
	defaultPageSize                             = 1000
	defaultPagesPerExecution                    = 256
	defaultConcurrentDeleteExecutionsActivities = 4
	maxConcurrentDeleteExecutionsActivities     = 256
)

type (
	DeleteExecutionsConfig struct {
		// Deprecated.
		// TODO: Remove after 1.27 release. RPS is now read directly by activity from config.
		DeleteActivityRPS int
		// Page size to read executions from visibility.
		PageSize int
		// Number of pages before returning ContinueAsNew.
		PagesPerExecution int
		// Number of concurrent delete executions activities.
		// Must be not greater than PagesPerExecution and number of worker cores in the cluster.
		ConcurrentDeleteExecutionsActivities int
	}
)

func (cfg *DeleteExecutionsConfig) ApplyDefaults() {
	if cfg.DeleteActivityRPS <= 0 {
		cfg.DeleteActivityRPS = defaultDeleteActivityRPS
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = defaultPageSize
	}
	if cfg.PagesPerExecution <= 0 {
		cfg.PagesPerExecution = defaultPagesPerExecution
	}
	if cfg.ConcurrentDeleteExecutionsActivities <= 0 {
		cfg.ConcurrentDeleteExecutionsActivities = defaultConcurrentDeleteExecutionsActivities
	}
	if cfg.ConcurrentDeleteExecutionsActivities > maxConcurrentDeleteExecutionsActivities {
		cfg.ConcurrentDeleteExecutionsActivities = maxConcurrentDeleteExecutionsActivities
	}
	// It won't be able to start more than PagesPerExecution activities.
	if cfg.ConcurrentDeleteExecutionsActivities > cfg.PagesPerExecution {
		cfg.ConcurrentDeleteExecutionsActivities = cfg.PagesPerExecution
	}
}

func (cfg *DeleteExecutionsConfig) String() string {
	cfgBytes, _ := json.Marshal(cfg)
	return string(cfgBytes)
}
