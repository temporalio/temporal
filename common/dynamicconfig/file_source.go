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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination file_source_mock.go

package dynamicconfig

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"gopkg.in/yaml.v3"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ Source = (*fileSource)(nil)

const (
	minPollInterval = time.Second * 5
	fileMode        = 0644 // used for update config file
)

type (
	fileReader interface {
		Stat(src string) (os.FileInfo, error)
		ReadFile(src string) ([]byte, error)
	}

	// FileSourceConfig is the config for the file based dynamic config source.
	// It specifies where the config file is stored and how often the config should be
	// updated by checking the config file again.
	FileSourceConfig struct {
		Filepath     string        `yaml:"filepath"`
		PollInterval time.Duration `yaml:"pollInterval"`
	}

	// FIXME: move to another file?
	configValueMap map[string][]ConstrainedValue

	fileSource struct {
		values          atomic.Value // configValueMap
		logger          log.Logger
		reader          fileReader
		lastUpdatedTime time.Time
		config          *FileSourceConfig
		doneCh          <-chan interface{}
	}

	osReader struct {
	}
)

// NewFileSource creates a file based source.
func NewFileSource(config *FileSourceConfig, logger log.Logger, doneCh <-chan interface{}) (*fileSource, error) {
	source := &fileSource{
		logger: logger,
		reader: &osReader{},
		config: config,
		doneCh: doneCh,
	}

	err := source.init()
	if err != nil {
		return nil, err
	}

	return source, nil
}

func NewFileSourceWithReader(reader fileReader, config *FileSourceConfig, logger log.Logger, doneCh <-chan interface{}) (*fileSource, error) {
	source := &fileSource{
		logger: logger,
		reader: reader,
		config: config,
		doneCh: doneCh,
	}

	err := source.init()
	if err != nil {
		return nil, err
	}

	return source, nil
}

func (fc *fileSource) GetValue(key Key) []ConstrainedValue {
	values := fc.values.Load().(configValueMap)
	return values[strings.ToLower(key.String())]
}

func (fc *fileSource) init() error {
	if err := fc.validateConfig(fc.config); err != nil {
		return fmt.Errorf("unable to validate dynamic config: %w", err)
	}

	if err := fc.update(); err != nil {
		return fmt.Errorf("unable to read dynamic config: %w", err)
	}

	go func() {
		ticker := time.NewTicker(fc.config.PollInterval)
		for {
			select {
			case <-ticker.C:
				err := fc.update()
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

func (fc *fileSource) update() error {
	defer func() {
		fc.lastUpdatedTime = time.Now().UTC()
	}()

	info, err := fc.reader.Stat(fc.config.Filepath)
	if err != nil {
		return fmt.Errorf("dynamic config file: %s: %w", fc.config.Filepath, err)
	}
	if !info.ModTime().After(fc.lastUpdatedTime) {
		return nil
	}

	confContent, err := fc.reader.ReadFile(fc.config.Filepath)
	if err != nil {
		return fmt.Errorf("dynamic config file: %s: %w", fc.config.Filepath, err)
	}

	var yamlValues map[string][]struct {
		Constraints map[string]any
		Value       any
	}
	if err = yaml.Unmarshal(confContent, &yamlValues); err != nil {
		return fmt.Errorf("unable to decode dynamic config: %w", err)
	}

	newValues := make(configValueMap, len(yamlValues))
	for key, yamlCV := range yamlValues {
		cvs := make([]ConstrainedValue, len(yamlCV))
		for i, cv := range yamlCV {
			// yaml will unmarshal map into map[interface{}]interface{} instead of map[string]interface{}
			// manually convert key type to string for all values here
			cvs[i].Value, err = convertKeyTypeToString(cv.Value)
			if err != nil {
				return err
			}
			cvs[i].Constraints, err = convertYamlConstraints(cv.Constraints)
			if err != nil {
				return err
			}
		}
		newValues[strings.ToLower(key)] = cvs
	}

	fc.values.Store(newValues)
	fc.logger.Info("Updated dynamic config")

	return nil
}

func (fc *fileSource) validateConfig(config *FileSourceConfig) error {
	if config == nil {
		return errors.New("configuration for dynamic config client is nil")
	}
	if _, err := fc.reader.Stat(config.Filepath); err != nil {
		return fmt.Errorf("dynamic config: %s: %w", config.Filepath, err)
	}
	if config.PollInterval < minPollInterval {
		return fmt.Errorf("poll interval should be at least %v", minPollInterval)
	}
	return nil
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

func convertYamlConstraints(m map[string]any) (Constraints, error) {
	var cs Constraints
	for k, v := range m {
		switch strings.ToLower(k) {
		case "namespace":
			if v, ok := v.(string); ok {
				cs.Namespace = v
			} else {
				return cs, fmt.Errorf("namespace constraint must be string")
			}
		case "namespaceid":
			if v, ok := v.(string); ok {
				cs.NamespaceID = v
			} else {
				return cs, fmt.Errorf("namespaceID constraint must be string")
			}
		case "taskqueuename":
			if v, ok := v.(string); ok {
				cs.TaskQueueName = v
			} else {
				return cs, fmt.Errorf("taskQueueName constraint must be string")
			}
		case "tasktype":
			if v, ok := v.(int); ok {
				switch v {
				case 0:
					cs.TaskQueueType = enumspb.TASK_QUEUE_TYPE_WORKFLOW
				case 1:
					cs.TaskQueueType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
				default:
					return cs, fmt.Errorf("taskType constraint must be 0 or 1")
				}
			} else {
				return cs, fmt.Errorf("taskType constraint must be 0 or 1")
			}
		case "shardid":
			if v, ok := v.(int); ok {
				cs.ShardID = int32(v)
			} else {
				return cs, fmt.Errorf("shardID constraint must be integer")
			}
		default:
			return cs, fmt.Errorf("unknown constraint type %q", k)
		}
	}
	return cs, nil
}

func (r *osReader) ReadFile(src string) ([]byte, error) {
	return os.ReadFile(src)
}

func (r *osReader) Stat(src string) (os.FileInfo, error) {
	return os.Stat(src)
}
