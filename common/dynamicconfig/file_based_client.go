//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination file_based_client_mock.go
package dynamicconfig

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
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

	fileBasedClient struct {
		values          atomic.Value // ConfigValueMap
		logger          log.Logger
		reader          FileReader
		lastCheckedTime time.Time
		config          *FileBasedClientConfig
		doneCh          <-chan any
		metricsHandler  metrics.Handler

		NotifyingClientImpl
	}

	osReader struct {
		path string
	}
)

// NewFileBasedClient creates a file based client.
func NewFileBasedClient(config *FileBasedClientConfig, logger log.Logger, doneCh <-chan any, metricsHandler metrics.Handler) (*fileBasedClient, error) {
	if config == nil {
		return nil, errors.New("configuration for dynamic config client is nil")
	}
	reader := &osReader{path: config.Filepath}
	return NewFileBasedClientWithReader(reader, config, logger, doneCh, metricsHandler)
}

func NewFileBasedClientWithReader(reader FileReader, config *FileBasedClientConfig, logger log.Logger, doneCh <-chan any, metricsHandler metrics.Handler) (*fileBasedClient, error) {
	client := &fileBasedClient{
		logger:              logger,
		reader:              reader,
		config:              config,
		doneCh:              doneCh,
		metricsHandler:      metricsHandler,
		NotifyingClientImpl: NewNotifyingClientImpl(),
	}

	err := client.init()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (fc *fileBasedClient) GetValue(key Key) []ConstrainedValue {
	values := fc.values.Load().(ConfigValueMap) // nolint:revive // unchecked-type-assertion
	return values[key]
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
func (fc *fileBasedClient) Update() (updateErr error) {
	modtime, err := fc.reader.GetModTime()
	retryOnErr := true
	if err != nil {
		return fmt.Errorf("dynamic config file: %s: %w", fc.config.Filepath, err)
	}
	if !modtime.After(fc.lastCheckedTime) {
		return nil
	}
	defer func() {
		if updateErr != nil {
			metrics.DynamicConfigUpdateFailureCounter.With(fc.metricsHandler).Record(1)
		}
		if updateErr == nil || !retryOnErr {
			fc.lastCheckedTime = modtime
		}
	}()

	contents, err := fc.reader.ReadFile()
	if err != nil {
		return fmt.Errorf("dynamic config file: %s: %w", fc.config.Filepath, err)
	}

	lr := LoadYamlFile(contents)
	for _, e := range lr.Errors {
		fc.logger.Error("dynamic config error", tag.Error(e))
	}
	for _, w := range lr.Warnings {
		fc.logger.Warn("dynamic config warning", tag.Error(w))
	}
	if len(lr.Errors) > 0 {
		retryOnErr = false
		return fmt.Errorf("loading dynamic config failed: %d errors, %d warnings",
			len(lr.Errors), len(lr.Warnings))
	}

	prev := fc.values.Swap(lr.Map)
	oldValues, _ := prev.(ConfigValueMap) // nolint:revive // unchecked-type-assertion
	changedMap := DiffAndLogConfigs(fc.logger, oldValues, lr.Map)
	fc.logger.Info("Updated dynamic config")

	fc.PublishUpdates(changedMap)
	return nil
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
