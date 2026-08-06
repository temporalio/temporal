package dynamicconfig

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.temporal.io/server/common/util"
)

func ConvertMatchingClientReadLoadBalancerMode(in any) (MatchingReadLoadBalancerMode, error) {
	var mode MatchingReadLoadBalancerMode
	switch value := in.(type) {
	case MatchingReadLoadBalancerMode:
		mode = value
	case string:
		mode = MatchingReadLoadBalancerMode(value)
	default:
		return "", errors.New("value type is not string")
	}
	switch mode {
	case MatchingReadLoadBalancerModeFewestPollers,
		MatchingReadLoadBalancerModeWeightedFewest,
		MatchingReadLoadBalancerModeBacklogWeighted:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown matching client read load balancer mode: %q", mode)
	}
}

var (
	MatchAnythingRE = regexp.MustCompile(".*")
	MatchNothingRE  = regexp.MustCompile(".^")
)

func ConvertWildcardStringListToRegexp(in any) (*regexp.Regexp, error) {
	// first convert raw value to list of strings
	var patterns []string
	if err := mapstructure.Decode(in, &patterns); err != nil {
		return nil, err
	}
	// then turn strings into regexp
	return util.WildCardStringsToRegexp(patterns)
}

func ConvertSimplePartitionScalerSettings(in any) (SimplePartitionScalerSettings, error) {
	cfg, err := ConvertStructure(SimplePartitionScalerSettings{})(in)
	if err != nil {
		return SimplePartitionScalerSettings{}, err
	}
	validateThreshold := func(t SimplePartitionScalerThreshold) error {
		if t.Window < 100*time.Millisecond {
			return errors.New("threshold window too small")
		} else if t.TargetRate < 1 {
			return errors.New("target rate too small")
		}
		return nil
	}
	if cfg.Fixed < 0 || cfg.Min < 0 || cfg.Max < 0 {
		return SimplePartitionScalerSettings{}, errors.New("negative value for Fixed/Min/Max")
	}
	for _, t := range cfg.Ups {
		if err := validateThreshold(t); err != nil {
			return SimplePartitionScalerSettings{}, err
		}
	}
	for _, t := range cfg.Downs {
		if err := validateThreshold(t); err != nil {
			return SimplePartitionScalerSettings{}, err
		}
	}
	return cfg, nil
}
