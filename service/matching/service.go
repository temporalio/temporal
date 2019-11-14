// Copyright (c) 2017 Uber Technologies, Inc.
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

package matching

import (
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	persistenceClient "github.com/uber/cadence/common/persistence/client"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Service represents the cadence-matching service
type Service struct {
	resource.Resource
	config *Config

	stopC chan struct{}
}

// NewService builds a new cadence-matching service
func NewService(
	params *service.BootstrapParams,
) (resource.Resource, error) {

	serviceConfig := NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger))
	serviceResource, err := resource.New(
		params,
		common.MatchingServiceName,
		serviceConfig.ThrottledLogRPS,
		func(
			persistenceBean persistenceClient.Bean,
			logger log.Logger,
		) (persistence.VisibilityManager, error) {
			return persistenceBean.GetVisibilityManager(), nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		Resource: serviceResource,
		config:   serviceConfig,
		stopC:    make(chan struct{}),
	}, nil
}

// Start starts the service
func (s *Service) Start() {

	logger := s.GetLogger()
	logger.Info("matching starting", tag.Service(common.MatchingServiceName))

	handler := NewHandler(s, s.config)
	handler.RegisterHandler()

	// must start base service first
	s.Resource.Start()
	handler.Start()

	logger.Info("matching started", tag.Service(common.MatchingServiceName))
	<-s.stopC
	s.Resource.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	close(s.stopC)
	s.GetLogger().Info("matching stopped", tag.Service(common.MatchingServiceName))
}
