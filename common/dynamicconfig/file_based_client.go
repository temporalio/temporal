// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination file_based_client_mock.go

package dynamicconfig

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	expmaps "golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

var _ Client = (*fileBasedClient)(nil)
var _ NotifyingClient = (*fileBasedClient)(nil)

const (
	minPollInterval = time.Second * 5
)

type (
	FileReader interface {
		GetModTime() (time.Time, error)
		ReadFile() ([]byte, error)
	}

	// FileBasedClientConfig is the config for the file based dynamic config client.
	// It specifies where the config file is stored and how often the config should be
	// updated by checking the config file again.
	FileBasedClientConfig struct {
		Filepath     string        `yaml:"filepath"`
		PollInterval time.Duration `yaml:"pollInterval"`
	}

	configValueMap map[string][]ConstrainedValue

	fileBasedClient struct {
		values          atomic.Value // configValueMap
		logger          log.Logger
		reader          FileReader
		lastUpdatedTime time.Time
		config          *FileBasedClientConfig
		doneCh          <-chan interface{}

		subscriptionLock sync.Mutex
		subscriptionIdx  int
		subscriptions    map[int]ClientUpdateFunc
	}

	osReader struct {
		path string
	}

	// Results of processing and loading dynamic config file contents.
	// Warnings should be reported but not block using the new values.
	// Errors should abort the loading process.
	LoadResult struct {
		Warnings []error
		Errors   []error
	}
)

func ValidateFile(contents []byte) *LoadResult {
	_, lr := loadFile(contents)
	return lr
}

// NewFileBasedClient creates a file based client.
func NewFileBasedClient(config *FileBasedClientConfig, logger log.Logger, doneCh <-chan interface{}) (*fileBasedClient, error) {
	if config == nil {
		return nil, errors.New("configuration for dynamic config client is nil")
	}
	reader := &osReader{path: config.Filepath}
	return NewFileBasedClientWithReader(reader, config, logger, doneCh)
}

func NewFileBasedClientWithReader(reader FileReader, config *FileBasedClientConfig, logger log.Logger, doneCh <-chan interface{}) (*fileBasedClient, error) {
	client := &fileBasedClient{
		logger:        logger,
		reader:        reader,
		config:        config,
		doneCh:        doneCh,
		subscriptions: make(map[int]ClientUpdateFunc),
	}

	err := client.init()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (fc *fileBasedClient) GetValue(key Key) []ConstrainedValue {
	values := fc.values.Load().(configValueMap)
	return values[strings.ToLower(key.String())]
}

func (fc *fileBasedClient) Subscribe(f ClientUpdateFunc) (cancel func()) {
	fc.subscriptionLock.Lock()
	defer fc.subscriptionLock.Unlock()

	fc.subscriptionIdx++
	id := fc.subscriptionIdx
	fc.subscriptions[id] = f

	return func() {
		fc.subscriptionLock.Lock()
		defer fc.subscriptionLock.Unlock()
		delete(fc.subscriptions, id)
	}
}

func (fc *fileBasedClient) init() error {
	if err := fc.validateStaticConfig(fc.config); err != nil {
		return fmt.Errorf("unable to validate dynamic config: %w", err)
	}

	if err := fc.Update(); err != nil {
		return fmt.Errorf("unable to read dynamic config: %w", err)
	}

	go func() {
		ticker := time.NewTicker(fc.config.PollInterval)
		for {
			select {
			case <-ticker.C:
				err := fc.Update()
				if err != nil {
					fc.logger.Error("Unable to update dynamic config.", tag.Error(err))
				}
			case <-fc.doneCh:
				ticker.Stop()
				return
			}
		}
	}()

	return nil
}

// This is public mainly for testing. The update loop will call this periodically, you don't
// have to call it explicitly.
func (fc *fileBasedClient) Update() error {
	modtime, err := fc.reader.GetModTime()
	if err != nil {
		return fmt.Errorf("dynamic config file: %s: %w", fc.config.Filepath, err)
	}
	if !modtime.After(fc.lastUpdatedTime) {
		return nil
	}
	fc.lastUpdatedTime = modtime

	contents, err := fc.reader.ReadFile()
	if err != nil {
		return fmt.Errorf("dynamic config file: %s: %w", fc.config.Filepath, err)
	}

	newValues, lr := loadFile(contents)
	for _, e := range lr.Errors {
		fc.logger.Warn("dynamic config error", tag.Error(e))
	}
	for _, w := range lr.Warnings {
		fc.logger.Warn("dynamic config warning", tag.Error(w))
	}
	if len(lr.Errors) > 0 {
		return fmt.Errorf("loading dynamic config failed: %d errors, %d warnings",
			len(lr.Errors), len(lr.Warnings))
	}

	prev := fc.values.Swap(newValues)
	oldValues, _ := prev.(configValueMap)
	changedMap := fc.diffAndLog(oldValues, newValues)
	fc.logger.Info("Updated dynamic config")

	if len(changedMap) == 0 {
		return nil
	}

	fc.subscriptionLock.Lock()
	subscriptions := expmaps.Values(fc.subscriptions)
	fc.subscriptionLock.Unlock()

	for _, update := range subscriptions {
		update(changedMap)
	}

	return nil
}

func loadFile(contents []byte) (configValueMap, *LoadResult) {
	lr := &LoadResult{}

	var yamlValues map[string][]struct {
		Constraints map[string]any
		Value       any
	}
	if err := yaml.Unmarshal(contents, &yamlValues); err != nil {
		return nil, lr.errorf("decode error: %w", err)
	}

	newValues := make(configValueMap, len(yamlValues))
	for key, yamlCV := range yamlValues {
		precedence := PrecedenceUnknown
		setting := queryRegistry(Key(key))
		if setting == nil {
			lr.warnf("unregistered key %q", key)
		} else {
			precedence = setting.Precedence()
		}

		cvs := make([]ConstrainedValue, len(yamlCV))
		for i, cv := range yamlCV {
			// yaml will unmarshal map into map[interface{}]interface{} instead of map[string]interface{}
			// manually convert key type to string for all values here
			val, err := convertKeyTypeToString(cv.Value)
			if err != nil {
				lr.error(err)
				continue
			}

			// try validating if known setting
			if setting != nil {
				if valErr := setting.Validate(val); valErr != nil {
					// TODO: raise this to error level
					lr.warnf("validation failed: key %q value %v: %w", key, cv.Value, valErr)
				}
			}

			cvs[i].Value = val
			cvs[i].Constraints = convertYamlConstraints(key, cv.Constraints, precedence, lr)
		}
		newValues[strings.ToLower(key)] = cvs
	}

	return newValues, lr
}

func (fc *fileBasedClient) validateStaticConfig(config *FileBasedClientConfig) error {
	if config == nil {
		return errors.New("configuration for dynamic config client is nil")
	}
	if _, err := fc.reader.GetModTime(); err != nil {
		return fmt.Errorf("dynamic config: %s: %w", config.Filepath, err)
	}
	if config.PollInterval < minPollInterval {
		return fmt.Errorf("poll interval should be at least %v", minPollInterval)
	}
	return nil
}

func (fc *fileBasedClient) diffAndLog(old configValueMap, new configValueMap) map[Key][]ConstrainedValue {
	changedMap := make(map[Key][]ConstrainedValue)

	for key, newValues := range new {
		oldValues, ok := old[key]
		if !ok {
			for _, newValue := range newValues {
				// new key added
				fc.diffAndLogValue(key, nil, &newValue)
			}
			changedMap[Key(key)] = newValues
		} else {
			// compare existing keys
			changed := fc.diffAndLogConstraints(key, oldValues, newValues)
			if changed {
				changedMap[Key(key)] = newValues
			}
		}
	}

	// check for removed values
	for key, oldValues := range old {
		if _, ok := new[key]; !ok {
			for _, oldValue := range oldValues {
				fc.diffAndLogValue(key, &oldValue, nil)
			}
			changedMap[Key(key)] = nil
		}
	}

	return changedMap
}

func (fc *fileBasedClient) diffAndLogConstraints(key string, oldValues []ConstrainedValue, newValues []ConstrainedValue) bool {
	changed := false
	for _, oldValue := range oldValues {
		matchFound := false
		for _, newValue := range newValues {
			if oldValue.Constraints == newValue.Constraints {
				matchFound = true
				if !reflect.DeepEqual(oldValue.Value, newValue.Value) {
					fc.diffAndLogValue(key, &oldValue, &newValue)
					changed = true
				}
			}
		}
		if !matchFound {
			fc.diffAndLogValue(key, &oldValue, nil)
			changed = true
		}
	}

	for _, newValue := range newValues {
		matchFound := false
		for _, oldValue := range oldValues {
			if oldValue.Constraints == newValue.Constraints {
				matchFound = true
			}
		}
		if !matchFound {
			fc.diffAndLogValue(key, nil, &newValue)
			changed = true
		}
	}
	return changed
}

func (fc *fileBasedClient) diffAndLogValue(key string, oldValue *ConstrainedValue, newValue *ConstrainedValue) {
	logLine := &strings.Builder{}
	logLine.Grow(128)
	logLine.WriteString("dynamic config changed for the key: ")
	logLine.WriteString(key)
	logLine.WriteString(" oldValue: ")
	fc.appendConstrainedValue(logLine, oldValue)
	logLine.WriteString(" newValue: ")
	fc.appendConstrainedValue(logLine, newValue)
	fc.logger.Info(logLine.String())
}

func (fc *fileBasedClient) appendConstrainedValue(logLine *strings.Builder, value *ConstrainedValue) {
	if value == nil {
		logLine.WriteString("nil")
	} else {
		logLine.WriteString("{ constraints: {")
		if value.Constraints.Namespace != "" {
			logLine.WriteString(fmt.Sprintf("{Namespace:%s}", value.Constraints.Namespace))
		}
		if value.Constraints.NamespaceID != "" {
			logLine.WriteString(fmt.Sprintf("{NamespaceID:%s}", value.Constraints.NamespaceID))
		}
		if value.Constraints.TaskQueueName != "" {
			logLine.WriteString(fmt.Sprintf("{TaskQueueName:%s}", value.Constraints.TaskQueueName))
		}
		if value.Constraints.TaskQueueType != enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
			logLine.WriteString(fmt.Sprintf("{TaskQueueType:%s}", value.Constraints.TaskQueueType))
		}
		if value.Constraints.ShardID != 0 {
			logLine.WriteString(fmt.Sprintf("{ShardID:%d}", value.Constraints.ShardID))
		}
		if value.Constraints.TaskType != enumsspb.TASK_TYPE_UNSPECIFIED {
			logLine.WriteString(fmt.Sprintf("{HistoryTaskType:%s}", value.Constraints.TaskType))
		}
		if value.Constraints.Destination != "" {
			logLine.WriteString(fmt.Sprintf("{Destination:%s}", value.Constraints.Destination))
		}
		logLine.WriteString(fmt.Sprint("} value: ", value.Value, " }"))
	}
}

func convertKeyTypeToString(v interface{}) (interface{}, error) {
	switch v := v.(type) {
	case map[interface{}]interface{}:
		return convertKeyTypeToStringMap(v)
	case []interface{}:
		return convertKeyTypeToStringSlice(v)
	default:
		return v, nil
	}
}

func convertKeyTypeToStringMap(m map[interface{}]interface{}) (map[string]interface{}, error) {
	stringKeyMap := make(map[string]interface{})
	for key, value := range m {
		stringKey, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("type of key %v is not string", key)
		}
		convertedValue, err := convertKeyTypeToString(value)
		if err != nil {
			return nil, err
		}
		stringKeyMap[stringKey] = convertedValue
	}
	return stringKeyMap, nil
}

func convertKeyTypeToStringSlice(s []interface{}) ([]interface{}, error) {
	stringKeySlice := make([]interface{}, len(s))
	for idx, value := range s {
		convertedValue, err := convertKeyTypeToString(value)
		if err != nil {
			return nil, err
		}
		stringKeySlice[idx] = convertedValue
	}
	return stringKeySlice, nil
}

func convertYamlConstraints(key string, m map[string]any, precedence Precedence, lr *LoadResult) Constraints {
	var cs Constraints
	for k, v := range m {
		validConstraint := true
		switch strings.ToLower(k) {
		case "namespace":
			if v, ok := v.(string); ok {
				cs.Namespace = v
			} else {
				lr.errorf("namespace constraint must be string")
			}
			validConstraint = precedence == PrecedenceNamespace || precedence == PrecedenceTaskQueue || precedence == PrecedenceDestination
		case "namespaceid":
			if v, ok := v.(string); ok {
				cs.NamespaceID = v
			} else {
				lr.errorf("namespaceID constraint must be string")
			}
			validConstraint = precedence == PrecedenceNamespaceID
		case "taskqueuename":
			if v, ok := v.(string); ok {
				cs.TaskQueueName = v
			} else {
				lr.errorf("taskQueueName constraint must be string")
			}
			validConstraint = precedence == PrecedenceTaskQueue
		case "tasktype":
			switch v := v.(type) {
			case string:
				i, err := enumspb.TaskQueueTypeFromString(v)
				if err != nil {
					lr.errorf("invalid value for taskType: %w", err)
				} else if i <= enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
					lr.errorf("taskType constraint must be Workflow/Activity")
				}
				cs.TaskQueueType = i
			case int:
				if v > int(enumspb.TASK_QUEUE_TYPE_UNSPECIFIED) {
					cs.TaskQueueType = enumspb.TaskQueueType(v)
				} else {
					lr.errorf("taskType constraint must be Workflow/Activity")
				}
			default:
				lr.errorf("taskType constraint must be Workflow/Activity")
			}
			validConstraint = precedence == PrecedenceTaskQueue
		case "historytasktype":
			switch v := v.(type) {
			case string:
				tt, err := enumsspb.TaskTypeFromString(v)
				if err != nil {
					lr.errorf("invalid value for historytasktype constraint: %w", err)
				} else if tt <= enumsspb.TASK_TYPE_UNSPECIFIED {
					lr.errorf("historytasktype %s constraint is not supported", v)
				}
				cs.TaskType = tt
			case int:
				cs.TaskType = enumsspb.TaskType(v)
			default:
				lr.errorf("historytasktype %T constraint is not supported", v)
			}
			validConstraint = precedence == PrecedenceTaskType
		case "shardid":
			if v, ok := v.(int); ok {
				cs.ShardID = int32(v)
			} else {
				lr.errorf("shardID constraint must be integer")
			}
			validConstraint = precedence == PrecedenceShardID
		case "destination":
			if v, ok := v.(string); ok {
				cs.Destination = v
			} else {
				lr.errorf("destination constraint must be string")
			}
			validConstraint = precedence == PrecedenceDestination
		default:
			lr.errorf("unknown constraint type %q", k)
		}

		// don't log error for PrecedenceUnknown, we would already have logged for an
		// unregistered key above
		// TODO: raise this to error level
		if !validConstraint && precedence != PrecedenceUnknown {
			lr.warnf("constraint %q isn't valid for dynamic config key %q", k, key)
		}
	}
	return cs
}

func (r *osReader) ReadFile() ([]byte, error) {
	return os.ReadFile(r.path)
}

func (r *osReader) GetModTime() (time.Time, error) {
	fi, err := os.Stat(r.path)
	if err != nil {
		return time.Time{}, err
	}
	return fi.ModTime(), nil
}

func (lr *LoadResult) warn(err error) *LoadResult {
	lr.Warnings = append(lr.Warnings, err)
	return lr
}

func (lr *LoadResult) warnf(format string, args ...any) *LoadResult {
	return lr.warn(fmt.Errorf(format, args...))
}

func (lr *LoadResult) error(err error) *LoadResult {
	lr.Errors = append(lr.Errors, err)
	return lr
}

func (lr *LoadResult) errorf(format string, args ...any) *LoadResult {
	return lr.error(fmt.Errorf(format, args...))
}
