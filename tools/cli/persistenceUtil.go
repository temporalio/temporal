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

package cli

import (
	"log"

	"github.com/urfave/cli"

	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

// InitializePersistenceFactory returns an initialized persistence managers factory.
// The factory allows to easily initialize concrete persistence managers to execute commands against persistence layer
func InitializePersistenceFactory(c *cli.Context) persistenceClient.Factory {
	config := loadConfig(c)

	if err := config.Validate(); err != nil {
		log.Fatalf("config validation failed: %v", err)
	}

	params := resource.BootstrapParams{}
	params.Name = "cli"
	params.Logger = loggerimpl.NewLogger(config.Log.NewZapLogger())
	params.PersistenceConfig = config.Persistence

	doneC := make(chan struct{})
	dConfig, err := dynamicconfig.NewFileBasedClient(&config.DynamicConfigClient, params.Logger.WithTags(tag.Service(params.Name)), doneC)
	if err != nil {
		log.Printf("error creating file based dynamic config client, use no-op config client instead. error: %v", err)
		params.DynamicConfig = dynamicconfig.NewNopClient()
	}
	params.DynamicConfig = dConfig
	dc := dynamicconfig.NewCollection(params.DynamicConfig, params.Logger)

	clusterMetadata := config.ClusterMetadata

	logger := params.Logger.WithTags(tag.ComponentMetadataInitializer)
	factory := persistenceClient.NewFactory(
		&params.PersistenceConfig,
		dc.GetIntProperty(dynamicconfig.HistoryPersistenceMaxQPS, 3000),
		params.AbstractDatastoreFactory,
		clusterMetadata.CurrentClusterName,
		nil, // MetricsClient
		logger,
	)

	return factory
}
