package dynamicconfig

import (
	"errors"
	"regexp"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.temporal.io/server/common/util"
)

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
		return SimplePartitionScalerSettings{}, errors.New("negative value for {Fixed,Min,Max}")
	} else if cfg.FixedAsMultipleOfLegacy < 0 || cfg.MinAsMultipleOfLegacy < 0 || cfg.MaxAsMultipleOfLegacy < 0 {
		return SimplePartitionScalerSettings{}, errors.New("negative value for {Fixed,Min,Max}AsMultipleOfLegacy")
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
