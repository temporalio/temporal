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

package provider

import (
	"errors"

	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/filestore"
	"github.com/uber/cadence/common/service/config"
)

var (
	// ErrUnknownScheme is the error for unknown archiver scheme
	ErrUnknownScheme = errors.New("unknown archiver scheme")
	// ErrBootstrapContainerNotFound is the error for unable to find the bootstrap container given serviceName
	ErrBootstrapContainerNotFound = errors.New("unable to find bootstrap container for the given service name")
	// ErrArchiverConfigNotFound is the error for unable to find the config for an archiver given scheme
	ErrArchiverConfigNotFound = errors.New("unable to find archiver config for the given scheme")
)

type (
	// ArchiverProvider returns history or visibility archiver based on the scheme and serviceName.
	// The archiver for each combination of scheme and serviceName will be created only once and cached.
	ArchiverProvider interface {
		RegisterBootstrapContainer(
			serviceName string,
			historyContainer *archiver.HistoryBootstrapContainer,
			visibilityContainter *archiver.VisibilityBootstrapContainer,
		)
		GetHistoryArchiver(scheme string, serviceName string) (archiver.HistoryArchiver, error)
		GetVisibilityArchiver(scheme string, serviceName string) (archiver.VisibilityArchiver, error)
	}

	archiverProvider struct {
		historyArchiverConfigs    *config.HistoryArchiverProvider
		visibilityArchiverConfigs *config.VisibilityArchiverProvider

		// Key for the container is just serviceName
		historyContainers    map[string]*archiver.HistoryBootstrapContainer
		visibilityContainers map[string]*archiver.VisibilityBootstrapContainer

		// Key for the archiver is scheme + serviceName
		historyArchivers    map[string]archiver.HistoryArchiver
		visibilityArchivers map[string]archiver.VisibilityArchiver
	}
)

// NewArchiverProvider returns a new Archiver provider
func NewArchiverProvider(
	historyArchiverConfigs *config.HistoryArchiverProvider,
	visibilityArchiverConfigs *config.VisibilityArchiverProvider,
) ArchiverProvider {
	return &archiverProvider{
		historyArchiverConfigs:    historyArchiverConfigs,
		visibilityArchiverConfigs: visibilityArchiverConfigs,
	}
}

// RegisterBootstrapContainer stores the given bootstrap container given the serviceName
// The container should be registered when a service starts up and before GetArchiver() is ever called.
// Later calls to GetArchiver() will used the registered container to initialize new archivers.
// If this method is called multiple times for one serviceName, later calls will overwrite the container
// given in previous calls.
func (p *archiverProvider) RegisterBootstrapContainer(
	serviceName string,
	historyContainer *archiver.HistoryBootstrapContainer,
	visibilityContainter *archiver.VisibilityBootstrapContainer,
) {
	p.historyContainers[serviceName] = historyContainer
	p.visibilityContainers[serviceName] = visibilityContainter
}

func (p *archiverProvider) GetHistoryArchiver(scheme, serviceName string) (archiver.HistoryArchiver, error) {
	archiverKey := p.getArchiverKey(scheme, serviceName)
	if historyArchiver, ok := p.historyArchivers[archiverKey]; ok {
		return historyArchiver, nil
	}

	container, ok := p.historyContainers[serviceName]
	if !ok {
		return nil, ErrBootstrapContainerNotFound
	}

	switch scheme {
	case filestore.URIScheme:
		if p.historyArchiverConfigs.Filestore == nil {
			return nil, ErrArchiverConfigNotFound
		}
		historyArchiver, err := filestore.NewHistoryArchiver(*container, p.historyArchiverConfigs.Filestore)
		if err != nil {
			return nil, err
		}
		p.historyArchivers[archiverKey] = historyArchiver
		return historyArchiver, nil
	}
	return nil, ErrUnknownScheme
}

func (p *archiverProvider) GetVisibilityArchiver(scheme, serviceName string) (archiver.VisibilityArchiver, error) {
	archiverKey := p.getArchiverKey(scheme, serviceName)
	if visibilityArchiver, ok := p.visibilityArchivers[archiverKey]; ok {
		return visibilityArchiver, nil
	}

	container, ok := p.visibilityContainers[serviceName]
	if !ok {
		return nil, ErrBootstrapContainerNotFound
	}

	switch scheme {
	case filestore.URIScheme:
		if p.visibilityArchiverConfigs.Filestore == nil {
			return nil, ErrArchiverConfigNotFound
		}
		p.visibilityArchivers[archiverKey] = filestore.NewVisibilityArchiver(*container, p.visibilityArchiverConfigs.Filestore)
		return p.visibilityArchivers[archiverKey], nil
	}
	return nil, ErrUnknownScheme
}

func (p *archiverProvider) getArchiverKey(scheme, serviceName string) string {
	return scheme + ":" + serviceName
}
