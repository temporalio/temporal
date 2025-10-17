package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/olivere/elastic/v7"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

const templateName = "temporal_visibility_v1_template"

type SetupConfig struct {
	TemplateContent string
	SettingsContent string
	VisibilityIndex string
	FailSilently    bool
}

type SetupTask struct {
	esClient client.CLIClient
	config   *SetupConfig
	logger   log.Logger
}

// setupClusterSettings handles cluster settings configuration
func (task *SetupTask) setupClusterSettings() error {
	config := task.config
	if len(config.SettingsContent) == 0 {
		task.logger.Info("Skipping cluster settings update, no embedded settings content")
		return nil
	}

	success, err := task.esClient.ClusterPutSettings(context.TODO(), config.SettingsContent)
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
	if len(config.TemplateContent) == 0 {
		task.logger.Info("Skipping template creation, no embedded template content")
		return nil
	}

	success, err := task.esClient.IndexPutTemplate(context.TODO(), templateName, config.TemplateContent)
	if err != nil {
		return task.handleOperationFailure("template creation failed", err)
	} else if !success {
		return task.handleOperationFailure("template creation failed without error", errors.New("acknowledged=false"))
	}

	task.logger.Info("Template created successfully", tag.NewStringTag("templateName", templateName))
	return nil
}

// setupIndex handles index creation. It checks if the index exists and skips creation if it does.
func (task *SetupTask) setupIndex(ctx context.Context) error {
	config := task.config
	if len(config.VisibilityIndex) == 0 {
		task.logger.Info("Skipping index creation, missing index name")
		return nil
	}

	success, err := task.esClient.CreateIndex(ctx, config.VisibilityIndex, nil)
	if err != nil {
		// Check if the error is an Elasticsearch error and if so check if the index already exists.
		var esErr *elastic.Error
		if errors.As(err, &esErr) {
			if esErr.Status == 400 && esErr.Details != nil && esErr.Details.Type == "resource_already_exists_exception" {
				task.logger.Info("Index already exists, skipping creation", tag.NewStringTag("indexName", config.VisibilityIndex))
				return nil
			}
		}
		return task.handleOperationFailure("index creation failed", err)
	} else if !success {
		return task.handleOperationFailure("index creation failed without error", errors.New("acknowledged=false"))
	}

	task.logger.Info("Index created successfully", tag.NewStringTag("indexName", config.VisibilityIndex))
	return nil
}

// RunSchemaSetup runs only cluster settings and template setup (no index creation)
func (task *SetupTask) RunSchemaSetup() error {
	task.logger.Info("Starting schema setup (cluster settings and template)", tag.NewAnyTag("config", task.config))

	if err := task.setupClusterSettings(); err != nil {
		task.logger.Error("Failed to setup cluster settings.", tag.Error(err))
		return err
	}

	if err := task.setupTemplate(); err != nil {
		task.logger.Error("Failed to setup template.", tag.Error(err))
		return err
	}

	task.logger.Info("Schema setup complete (cluster settings and template)")
	return nil
}

// RunTemplateUpgrade runs only template upgrade
func (task *SetupTask) RunTemplateUpgrade() error {
	task.logger.Info("Starting template upgrade", tag.NewAnyTag("config", task.config))

	if err := task.setupTemplate(); err != nil {
		task.logger.Error("Failed to upgrade template.", tag.Error(err))
		return err
	}

	task.logger.Info("Template upgrade complete")
	return nil
}

// RunIndexCreation runs only index creation
func (task *SetupTask) RunIndexCreation(ctx context.Context) error {
	task.logger.Info("Starting index creation", tag.NewAnyTag("config", task.config))

	if err := task.setupIndex(ctx); err != nil {
		task.logger.Error("Failed to create index.", tag.Error(err))
		return err
	}

	task.logger.Info("Index creation complete")
	return nil
}

// RunIndexUpdate updates the mappings of an existing index
func (task *SetupTask) RunIndexUpdate() error {
	task.logger.Info("Starting index mapping update", tag.NewAnyTag("config", task.config))

	if err := task.updateIndexMappings(); err != nil {
		task.logger.Error("Failed to update index mappings.", tag.Error(err))
		return err
	}

	task.logger.Info("Index mapping update complete")
	return nil
}

// updateIndexMappings updates the mappings of an existing index using raw HTTP request
func (task *SetupTask) updateIndexMappings() error {
	config := task.config
	if len(config.VisibilityIndex) == 0 {
		task.logger.Info("Skipping index mapping update, missing index name")
		return nil
	}

	if len(config.TemplateContent) == 0 {
		task.logger.Info("Skipping index mapping update, no embedded template content")
		return nil
	}

	// Parse the template to extract mappings
	var template map[string]interface{}
	if err := json.Unmarshal([]byte(config.TemplateContent), &template); err != nil {
		return fmt.Errorf("failed to parse template content: %w", err)
	}

	mappings, ok := template["mappings"]
	if !ok {
		return errors.New("no mappings found in template")
	}

	mappingsBytes, err := json.Marshal(mappings)
	if err != nil {
		return fmt.Errorf("failed to marshal mappings: %w", err)
	}

	// Check if the index exists first
	indexName := config.VisibilityIndex
	exists, err := task.esClient.IndexExists(context.TODO(), indexName)
	if err != nil {
		return task.handleOperationFailure("failed to check if index exists", err)
	}
	if !exists {
		return task.handleOperationFailure("index does not exist", fmt.Errorf("index %s does not exist", indexName))
	}

	success, err := task.esClient.IndexPutMapping(context.TODO(), indexName, string(mappingsBytes))
	if err != nil {
		return task.handleOperationFailure("index mapping update failed", err)
	} else if !success {
		return task.handleOperationFailure("index mapping update failed without error", errors.New("acknowledged=false"))
	}

	task.logger.Info("Index mappings updated successfully", tag.NewStringTag("indexName", indexName))
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
