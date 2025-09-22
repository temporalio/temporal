package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

const templateName = "temporal_visibility_v1_template"

type SetupConfig struct {
	TemplateFilePath string
	SettingsFilePath string
	VisibilityIndex  string
	FailSilently     bool
}

type SetupTask struct {
	esClient client.CLIClient
	config   *SetupConfig
	logger   log.Logger
}

// Run executes the task
func (task *SetupTask) Run() error {
	task.logger.Info("Starting schema setup", tag.NewAnyTag("config", task.config))

	if err := task.setupClusterSettings(); err != nil {
		task.logger.Error("Failed to setup cluster settings.", tag.Error(err))
		return err
	}

	if err := task.setupTemplate(); err != nil {
		task.logger.Error("Failed to setup template.", tag.Error(err))
		return err
	}

	if err := task.setupIndex(); err != nil {
		task.logger.Error("Failed to setup index.", tag.Error(err))
		return err
	}

	task.logger.Info("Schema setup complete")
	return nil
}

// setupClusterSettings handles cluster settings configuration
func (task *SetupTask) setupClusterSettings() error {
	config := task.config
	if len(config.SettingsFilePath) == 0 {
		task.logger.Info("Skipping cluster settings update, missing " + flag(CLIOptSettingsFile))
		return nil
	}

	filePath, err := filepath.Abs(config.SettingsFilePath)
	if err != nil {
		task.logger.Error("Failed to resolve settings file path.", tag.Error(err), tag.NewStringTag("settingsFile", config.SettingsFilePath))
		return fmt.Errorf("failed to resolve settings file path: %w", err)
	}

	body, err := os.ReadFile(filePath)
	if err != nil {
		task.logger.Error("Failed to read settings file.", tag.Error(err), tag.NewStringTag("filePath", filePath))
		return fmt.Errorf("failed to read settings file: %w", err)
	}

	success, err := task.esClient.ClusterPutSettings(context.TODO(), string(body))
	if err != nil {
		return task.handleOperationFailure("cluster settings update failed", err)
	} else if !success {
		return task.handleOperationFailure("cluster settings update failed without error", errors.New("acknowledged=false"))
	}

	task.logger.Info("Cluster settings updated successfully")
	return nil
}

// setupTemplate handles template configuration
func (task *SetupTask) setupTemplate() error {
	config := task.config
	if len(config.TemplateFilePath) == 0 {
		task.logger.Info("Skipping template creation, missing " + flag(CLIOptTemplateFile))
		return nil
	}

	filePath, err := filepath.Abs(config.TemplateFilePath)
	if err != nil {
		task.logger.Error("Failed to resolve template file path.", tag.Error(err), tag.NewStringTag("templateFile", config.TemplateFilePath))
		return fmt.Errorf("failed to resolve template file path: %w", err)
	}

	body, err := os.ReadFile(filePath)
	if err != nil {
		task.logger.Error("Failed to read template file.", tag.Error(err), tag.NewStringTag("filePath", filePath))
		return fmt.Errorf("failed to read template file: %w", err)
	}

	success, err := task.esClient.IndexPutTemplate(context.TODO(), templateName, string(body))
	if err != nil {
		return task.handleOperationFailure("template creation failed", err)
	} else if !success {
		return task.handleOperationFailure("template creation failed without error", errors.New("acknowledged=false"))
	}

	task.logger.Info("Template created successfully", tag.NewStringTag("templateName", templateName))
	return nil
}

// setupIndex handles index creation
func (task *SetupTask) setupIndex() error {
	config := task.config
	if len(config.VisibilityIndex) == 0 {
		task.logger.Info("Skipping index creation, missing " + flag(CLIOptVisibilityIndex))
		return nil
	}

	success, err := task.esClient.CreateIndex(context.TODO(), config.VisibilityIndex, nil)
	if err != nil {
		return task.handleOperationFailure("index creation failed", err)
	} else if !success {
		return task.handleOperationFailure("index creation failed without error", errors.New("acknowledged=false"))
	}

	task.logger.Info("Index created successfully", tag.NewStringTag("indexName", config.VisibilityIndex))
	return nil
}

// handleOperationFailure handles operation failures, optionally failing silently
func (task *SetupTask) handleOperationFailure(msg string, err error) error {
	if !task.config.FailSilently {
		task.logger.Error(msg, tag.Error(err))
		return err
	}
	task.logger.Warn(msg, tag.Error(err))
	return nil
}
