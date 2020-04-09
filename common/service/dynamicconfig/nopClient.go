package dynamicconfig

import (
	"errors"
	"time"

	"github.com/temporalio/temporal/common/log"
)

// nopClient is a dummy implements of dynamicconfig Client interface, all operations will always return default values.
type nopClient struct{}

func (mc *nopClient) GetValue(name Key, defaultValue interface{}) (interface{}, error) {
	return nil, errors.New("unable to find key")
}

func (mc *nopClient) GetValueWithFilters(
	name Key, filters map[Filter]interface{}, defaultValue interface{},
) (interface{}, error) {
	return nil, errors.New("unable to find key")
}

func (mc *nopClient) GetIntValue(name Key, filters map[Filter]interface{}, defaultValue int) (int, error) {
	return defaultValue, errors.New("unable to find key")
}

func (mc *nopClient) GetFloatValue(name Key, filters map[Filter]interface{}, defaultValue float64) (float64, error) {
	return defaultValue, errors.New("unable to find key")
}

func (mc *nopClient) GetBoolValue(name Key, filters map[Filter]interface{}, defaultValue bool) (bool, error) {
	return defaultValue, errors.New("unable to find key")
}

func (mc *nopClient) GetStringValue(name Key, filters map[Filter]interface{}, defaultValue string) (string, error) {
	return defaultValue, errors.New("unable to find key")
}

func (mc *nopClient) GetMapValue(
	name Key, filters map[Filter]interface{}, defaultValue map[string]interface{},
) (map[string]interface{}, error) {
	return defaultValue, errors.New("unable to find key")
}

func (mc *nopClient) GetDurationValue(
	name Key, filters map[Filter]interface{}, defaultValue time.Duration,
) (time.Duration, error) {
	return defaultValue, errors.New("unable to find key")
}

func (mc *nopClient) UpdateValue(name Key, value interface{}) error {
	return errors.New("unable to update key")
}

// NewNopClient creates a nop client
func NewNopClient() Client {
	return &nopClient{}
}

// NewNopCollection creates a new nop collection
func NewNopCollection() *Collection {
	return NewCollection(&nopClient{}, log.NewNoop())
}
