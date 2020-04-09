//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination clientInterface_mock.go -self_package github.com/temporalio/temporal/common/service/dynamicconfig

package dynamicconfig

import (
	"time"
)

// Client allows fetching values from a dynamic configuration system NOTE: This does not have async
// options right now. In the interest of keeping it minimal, we can add when requirement arises.
type Client interface {
	GetValue(name Key, defaultValue interface{}) (interface{}, error)
	GetValueWithFilters(name Key, filters map[Filter]interface{}, defaultValue interface{}) (interface{}, error)

	GetIntValue(name Key, filters map[Filter]interface{}, defaultValue int) (int, error)
	GetFloatValue(name Key, filters map[Filter]interface{}, defaultValue float64) (float64, error)
	GetBoolValue(name Key, filters map[Filter]interface{}, defaultValue bool) (bool, error)
	GetStringValue(name Key, filters map[Filter]interface{}, defaultValue string) (string, error)
	GetMapValue(
		name Key, filters map[Filter]interface{}, defaultValue map[string]interface{},
	) (map[string]interface{}, error)
	GetDurationValue(
		name Key, filters map[Filter]interface{}, defaultValue time.Duration,
	) (time.Duration, error)
	// UpdateValue takes value as map and updates by overriding. It doesn't support update with filters.
	UpdateValue(name Key, value interface{}) error
}
