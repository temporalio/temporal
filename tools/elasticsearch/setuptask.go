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

package elasticsearch

import (
	"context"
	"io/ioutil"
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
	config := task.config
	task.logger.Info("Starting schema setup", tag.NewAnyTag("config", config))

	handleErr := func(msg string, err error) {
		if !config.FailSilently {
			task.logger.Fatal(msg, tag.Error(err))
		}
	}

	if len(config.SettingsFilePath) > 0 {
		filePath, err := filepath.Abs(config.SettingsFilePath)
		if err != nil {
			return err
		}
		body, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}

		success, err := task.esClient.ClusterPutSettings(context.TODO(), string(body))
		if err != nil && err.Error() != "" {
			handleErr("cluster settings update failed", err)
		} else if !success {
			handleErr("cluster settings update failed without error", nil)
		}
	} else {
		task.logger.Info("Skipping cluster settings update, missing " + flag(CLIOptSettingsFile))
	}

	if len(config.TemplateFilePath) > 0 {
		filePath, err := filepath.Abs(config.TemplateFilePath)
		if err != nil {
			return err
		}
		body, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}

		success, err := task.esClient.IndexPutTemplate(context.TODO(), templateName, string(body))
		if err != nil && err.Error() != "" {
			handleErr("template creation failed", err)
		} else if !success {
			handleErr("template creation failed without error", nil)
		}
	} else {
		task.logger.Info("Skipping template creation, missing " + flag(CLIOptTemplateFile))
	}

	if len(config.VisibilityIndex) > 0 {
		success, err := task.esClient.CreateIndex(context.TODO(), task.config.VisibilityIndex)
		if err != nil && err.Error() != "" {
			handleErr("index creation failed", err)
		} else if !success {
			handleErr("index creation failed without error", nil)
		}
	} else {
		task.logger.Info("Skipping index creation, missing " + flag(CLIOptVisibilityIndex))
	}

	task.logger.Info("Schema setup complete")

	return nil
}
