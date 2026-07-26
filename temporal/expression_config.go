package temporal

import (
	"fmt"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// expressionEvaluatorProvider builds the optional constraint-expression layer over dynamic
// config. It returns nil unless dynamicConfigClient.expressionFilepath is set, in which case
// dynamic config resolves exactly as it did before.
//
// PROTOTYPE: this is an experiment in constraint-expression configuration
// (see common/dynamicconfig/configurator/README.md), not a supported way to configure the
// server. Config in the expression file wins over the same key in the dynamic config file.
//
// Unlike the file based client, a failure to load at startup is fatal: silently running with
// compiled-in defaults when the operator asked for an expression file would be worse than
// refusing to start.
func expressionEvaluatorProvider(
	cfg *config.Config,
	logger log.Logger,
	stopChan chan any,
) (dynamicconfig.Evaluator, error) {
	dcConfig := cfg.DynamicConfigClient
	if dcConfig == nil || dcConfig.ExpressionFilepath == "" {
		return nil, nil
	}

	ambient := dynamicconfig.AmbientConstraints{
		ClusterName: cfg.ClusterMetadata.CurrentClusterName,
		Custom:      dcConfig.ExpressionConstraints,
	}
	if v, ok := dcConfig.ExpressionConstraints["env"].(string); ok {
		ambient.Environment = v
	}
	if v, ok := dcConfig.ExpressionConstraints["zone"].(string); ok {
		ambient.AvailabilityZone = v
	}

	evaluator := dynamicconfig.NewConfiguratorEvaluator(ambient, logger)
	if err := evaluator.LoadFileFrom(dcConfig.ExpressionFilepath); err != nil {
		return nil, fmt.Errorf("unable to load dynamic config expression file: %w", err)
	}
	logger.Info("Loaded dynamic config expression file",
		tag.NewStringTag("path", dcConfig.ExpressionFilepath))

	pollInterval := max(dcConfig.PollInterval, dynamicconfig.ExpressionFilePollInterval)
	stopWatching := evaluator.StartWatching(dcConfig.ExpressionFilepath, pollInterval)
	go func() {
		<-stopChan
		stopWatching()
	}()

	return evaluator, nil
}
