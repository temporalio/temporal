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

package dynamicconfig

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ Client = (*fileBasedClient)(nil)

const (
	minPollInterval = time.Second * 5
	fileMode        = 0644 // used for update config file
)

type (
	// FileBasedClientConfig is the config for the file based dynamic config client.
	// It specifies where the config file is stored and how often the config should be
	// updated by checking the config file again.
	FileBasedClientConfig struct {
		Filepath     string        `yaml:"filepath"`
		PollInterval time.Duration `yaml:"pollInterval"`
	}

	fileBasedClient struct {
		*basicClient
		reader          FileReader
		lastUpdatedTime time.Time
		config          *FileBasedClientConfig
		doneCh          <-chan interface{}
		logger          log.Logger
	}

	osReader struct {
	}
)

var OsReader FileReader = &osReader{}

// NewFileBasedClient creates a file based client.
func NewFileBasedClient(config *FileBasedClientConfig, logger log.Logger, doneCh <-chan interface{}) (*fileBasedClient, error) {
	client := &fileBasedClient{
		basicClient: newBasicClient(),
		reader:      OsReader,
		config:      config,
		doneCh:      doneCh,
		logger:      logger,
	}

	err := client.init()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func NewFileBasedClientWithReader(reader FileReader, config *FileBasedClientConfig, logger log.Logger, doneCh <-chan interface{}) (*fileBasedClient, error) {
	client := &fileBasedClient{
		basicClient: newBasicClient(),
		reader:      reader,
		config:      config,
		doneCh:      doneCh,
		logger:      logger,
	}

	err := client.init()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (fc *fileBasedClient) init() error {
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

func (fc *fileBasedClient) update() error {
	defer func() {
		fc.lastUpdatedTime = time.Now().UTC()
	}()

	newValues := make(configValueMap)

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

	if err = yaml.Unmarshal(confContent, newValues); err != nil {
		return fmt.Errorf("unable to decode dynamic config: %w", err)
	}

	oldValues := fc.basicClient.getValues()
	err = fc.storeValues(newValues)
	newStoredValues := fc.getValues()

	fc.logDiff(oldValues, newStoredValues)

	return err
}

func (fc *fileBasedClient) logDiff(old configValueMap, new configValueMap) {
	for key, newValues := range new {
		oldValues, ok := old[key]
		if !ok {
			for _, newValue := range newValues {
				// new key added
				fc.logValueDiff(key, nil, newValue)
			}
		} else {
			// compare existing keys
			fc.logConstraintsDiff(key, oldValues, newValues)
		}
	}

	// check for removed values
	for key, oldValues := range old {
		if _, ok := new[key]; !ok {
			for _, oldValue := range oldValues {
				fc.logValueDiff(key, oldValue, nil)
			}
		}
	}
}

func (fc *fileBasedClient) logConstraintsDiff(key string, oldValues []*constrainedValue, newValues []*constrainedValue) {
	for _, oldValue := range oldValues {
		matchFound := false
		for _, newValue := range newValues {
			if reflect.DeepEqual(oldValue.Constraints, newValue.Constraints) {
				matchFound = true
				if !reflect.DeepEqual(oldValue.Value, newValue.Value) {
					fc.logValueDiff(key, oldValue, newValue)
				}
			}
		}
		if !matchFound {
			fc.logValueDiff(key, oldValue, nil)
		}
	}

	for _, newValue := range newValues {
		matchFound := false
		for _, oldValue := range oldValues {
			if reflect.DeepEqual(oldValue.Constraints, newValue.Constraints) {
				matchFound = true
			}
		}
		if !matchFound {
			fc.logValueDiff(key, nil, newValue)
		}
	}
}

func (fc *fileBasedClient) logValueDiff(key string, oldValue *constrainedValue, newValue *constrainedValue) {
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

func (fc *fileBasedClient) appendConstrainedValue(logLine *strings.Builder, value *constrainedValue) {
	if value == nil {
		logLine.WriteString("nil")
	} else {
		logLine.WriteString("{ constraints: {")
		for constraintKey, constraintValue := range value.Constraints {
			logLine.WriteString("{")
			logLine.WriteString(constraintKey)
			logLine.WriteString(":")
			logLine.WriteString(fmt.Sprintf("%v", constraintValue))
			logLine.WriteString("}")
		}
		logLine.WriteString(fmt.Sprint("} value: ", value.Value, " }"))
	}
}

func (fc *fileBasedClient) storeValues(newValues map[string][]*constrainedValue) error {
	formattedNewValues := make(configValueMap, len(newValues))

	// yaml will unmarshal map into map[interface{}]interface{} instead of map[string]interface{}
	// manually convert key type to string for all values here
	// We don't need to convert constraints as their type can't be map. If user does use a map as filter
	// value, it won't match anyway.
	for key, valuesSlice := range newValues {
		for _, cv := range valuesSlice {
			var err error
			cv.Value, err = convertKeyTypeToString(cv.Value)
			if err != nil {
				return err
			}
		}
		formattedNewValues[strings.ToLower(key)] = valuesSlice
	}

	fc.basicClient.updateValues(formattedNewValues)
	fc.logger.Info("Updated dynamic config")
	return nil
}

func (fc *fileBasedClient) validateConfig(config *FileBasedClientConfig) error {
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

func (r *osReader) ReadFile(src string) ([]byte, error) {
	return os.ReadFile(src)
}

func (r *osReader) Stat(src string) (os.FileInfo, error) {
	return os.Stat(src)
}
