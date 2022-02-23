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
	fileReader interface {
		Stat(src string) (os.FileInfo, error)
		ReadFile(src string) ([]byte, error)
	}

	// FileBasedClientConfig is the config for the file based dynamic config client.
	// It specifies where the config file is stored and how often the config should be
	// updated by checking the config file again.
	FileBasedClientConfig struct {
		Filepath     string        `yaml:"filepath"`
		PollInterval time.Duration `yaml:"pollInterval"`
	}

	fileBasedClient struct {
		*basicClient
		reader          fileReader
		lastUpdatedTime time.Time
		config          *FileBasedClientConfig
		doneCh          <-chan interface{}
	}

	osReader struct {
	}
)

// NewFileBasedClient creates a file based client.
func NewFileBasedClient(config *FileBasedClientConfig, logger log.Logger, doneCh <-chan interface{}) (*fileBasedClient, error) {
	client := &fileBasedClient{
		basicClient: newBasicClient(logger),
		reader:      &osReader{},
		config:      config,
		doneCh:      doneCh,
	}

	err := client.init()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func NewFileBasedClientWithReader(reader fileReader, config *FileBasedClientConfig, logger log.Logger, doneCh <-chan interface{}) (*fileBasedClient, error) {
	client := &fileBasedClient{
		basicClient: newBasicClient(logger),
		reader:      reader,
		config:      config,
		doneCh:      doneCh,
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

	err = fc.storeValues(newValues)

	return err
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
