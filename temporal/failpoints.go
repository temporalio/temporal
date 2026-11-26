package temporal

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/testx/failpoint"
)

// resolveFailpoints reads the static startup switch once and returns the failpoint registry that the
// server's services share. It returns nil unless the failpoints.enabled config setting (enabled) is
// set; nothing else, such as an environment variable or dynamic config, can enable failpoints.
// If it enables them, it logs a warning.
func resolveFailpoints(enabled bool, logger log.Logger) *failpoint.Registry {
	if !enabled {
		return nil
	}
	logger.Warn("FAILPOINTS ARE ENABLED (failpoints.enabled in the static config): tests and tools can make " +
		"this server fail, stall or return errors. This is not meant for production use.")
	return failpoint.NewRegistry()
}
