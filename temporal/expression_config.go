package temporal

import (
	"fmt"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/resource"
)

// withExpressionConfig optionally layers constraint-expression configuration over the
// dynamic config client. It returns inner unchanged unless
// dynamicConfigClient.expressionFilepath is set, so by default dynamic config resolves
// exactly as it did before.
//
// PROTOTYPE: an experiment in constraint-expression configuration
// (see common/dynamicconfig/configurator/README.md), not a supported way to configure the
// server. A key in the expression file is served from there; every other key comes from
// inner as usual.
//
// Unlike the file based client, a failure to load at startup is fatal: silently running
// with compiled-in defaults when the operator asked for an expression file would be worse
// than refusing to start.
func withExpressionConfig(
	cfg *config.Config,
	inner dynamicconfig.Client,
	serviceNames resource.ServiceNames,
	logger log.Logger,
	stopChan chan any,
) (dynamicconfig.Client, error) {
	dcConfig := cfg.DynamicConfigClient
	if dcConfig == nil || dcConfig.ExpressionFilepath == "" {
		return inner, nil
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
	// The client is shared by every service in this process, so "service" is only a
	// meaningful dimension when the process hosts exactly one.
	if len(serviceNames) == 1 {
		for name := range serviceNames {
			ambient.ServiceName = string(name)
		}
	}

	client := dynamicconfig.NewConfiguratorClient(ambient, inner, logger)
	if err := client.LoadFileFrom(dcConfig.ExpressionFilepath); err != nil {
		return nil, fmt.Errorf("unable to load dynamic config expression file: %w", err)
	}
	logger.Info("Loaded dynamic config expression file",
		tag.NewStringTag("path", dcConfig.ExpressionFilepath))

	pollInterval := max(dcConfig.PollInterval, dynamicconfig.ExpressionFilePollInterval)
	stopWatching := client.StartWatching(dcConfig.ExpressionFilepath, pollInterval)
	go func() {
		<-stopChan
		stopWatching()
		client.Stop()
	}()

	return client, nil
}
