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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination provider_mock.go

package provider

import (
	"errors"
	"sync"

	"go.temporal.io/server/common/archiver/gcloud"

	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/s3store"
	"go.temporal.io/server/common/config"
)

var (
	// ErrUnknownScheme is the error for unknown archiver scheme
	ErrUnknownScheme = errors.New("unknown archiver scheme")
	// ErrNotSupported is the error for not supported archiver implementation
	ErrNotSupported = errors.New("archiver provider not supported")
	// ErrBootstrapContainerNotFound is the error for unable to find the bootstrap container given serviceName
	ErrBootstrapContainerNotFound = errors.New("unable to find bootstrap container for the given service name")
	// ErrArchiverConfigNotFound is the error for unable to find the config for an archiver given scheme
	ErrArchiverConfigNotFound = errors.New("unable to find archiver config for the given scheme")
	// ErrBootstrapContainerAlreadyRegistered is the error for registering multiple containers for the same serviceName
	ErrBootstrapContainerAlreadyRegistered = errors.New("bootstrap container has already been registered")
)

type (
	// ArchiverProvider returns history or visibility archiver based on the scheme and serviceName.
	// The archiver for each combination of scheme and serviceName will be created only once and cached.
	ArchiverProvider interface {
		RegisterBootstrapContainer(
			serviceName string,
			historyContainer *archiver.HistoryBootstrapContainer,
			visibilityContainter *archiver.VisibilityBootstrapContainer,
		) error
		GetHistoryArchiver(scheme, serviceName string) (archiver.HistoryArchiver, error)
		GetVisibilityArchiver(scheme, serviceName string) (archiver.VisibilityArchiver, error)
	}

	archiverProvider struct {
		sync.RWMutex

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
		historyContainers:         make(map[string]*archiver.HistoryBootstrapContainer),
		visibilityContainers:      make(map[string]*archiver.VisibilityBootstrapContainer),
		historyArchivers:          make(map[string]archiver.HistoryArchiver),
		visibilityArchivers:       make(map[string]archiver.VisibilityArchiver),
	}
}

// RegisterBootstrapContainer stores the given bootstrap container given the serviceName
// The container should be registered when a service starts up and before GetArchiver() is ever called.
// Later calls to GetArchiver() will used the registered container to initialize new archivers.
// If the container for a service has already registered, and this method is invoked for that service again
// with an non-nil container, an error will be returned.
func (p *archiverProvider) RegisterBootstrapContainer(
	serviceName string,
	historyContainer *archiver.HistoryBootstrapContainer,
	visibilityContainter *archiver.VisibilityBootstrapContainer,
) error {
	p.Lock()
	defer p.Unlock()

	if _, ok := p.historyContainers[serviceName]; ok && historyContainer != nil {
		return ErrBootstrapContainerAlreadyRegistered
	}
	if _, ok := p.visibilityContainers[serviceName]; ok && visibilityContainter != nil {
		return ErrBootstrapContainerAlreadyRegistered
	}

	if historyContainer != nil {
		p.historyContainers[serviceName] = historyContainer
	}
	if visibilityContainter != nil {
		p.visibilityContainers[serviceName] = visibilityContainter
	}
	return nil
}

func (p *archiverProvider) GetHistoryArchiver(scheme, serviceName string) (historyArchiver archiver.HistoryArchiver, err error) {
	archiverKey := p.getArchiverKey(scheme, serviceName)
	p.RLock()
	if historyArchiver, ok := p.historyArchivers[archiverKey]; ok {
		p.RUnlock()
		return historyArchiver, nil
	}
	p.RUnlock()

	container, ok := p.historyContainers[serviceName]
	if !ok {
		return nil, ErrBootstrapContainerNotFound
	}

	switch scheme {
	case filestore.URIScheme:
		if p.historyArchiverConfigs.Filestore == nil {
			return nil, ErrArchiverConfigNotFound
		}
		historyArchiver, err = filestore.NewHistoryArchiver(container, p.historyArchiverConfigs.Filestore)

	case gcloud.URIScheme:
		if p.historyArchiverConfigs.Gstorage == nil {
			return nil, ErrArchiverConfigNotFound
		}

		historyArchiver, err = gcloud.NewHistoryArchiver(container, p.historyArchiverConfigs.Gstorage)

	case s3store.URIScheme:
		if p.historyArchiverConfigs.S3store == nil {
			return nil, ErrArchiverConfigNotFound
		}
		historyArchiver, err = s3store.NewHistoryArchiver(container, p.historyArchiverConfigs.S3store)
	default:
		return nil, ErrUnknownScheme
	}

	if err != nil {
		return nil, err
	}

	p.Lock()
	defer p.Unlock()
	if existingHistoryArchiver, ok := p.historyArchivers[archiverKey]; ok {
		return existingHistoryArchiver, nil
	}
	p.historyArchivers[archiverKey] = historyArchiver
	return historyArchiver, nil
}

func (p *archiverProvider) GetVisibilityArchiver(scheme, serviceName string) (archiver.VisibilityArchiver, error) {
	archiverKey := p.getArchiverKey(scheme, serviceName)
	p.RLock()
	if visibilityArchiver, ok := p.visibilityArchivers[archiverKey]; ok {
		p.RUnlock()
		return visibilityArchiver, nil
	}
	p.RUnlock()

	container, ok := p.visibilityContainers[serviceName]
	if !ok {
		return nil, ErrBootstrapContainerNotFound
	}

	var visibilityArchiver archiver.VisibilityArchiver
	var err error

	switch scheme {
	case filestore.URIScheme:
		if p.visibilityArchiverConfigs.Filestore == nil {
			return nil, ErrArchiverConfigNotFound
		}
		visibilityArchiver, err = filestore.NewVisibilityArchiver(container, p.visibilityArchiverConfigs.Filestore)
	case s3store.URIScheme:
		if p.visibilityArchiverConfigs.S3store == nil {
			return nil, ErrArchiverConfigNotFound
		}
		visibilityArchiver, err = s3store.NewVisibilityArchiver(container, p.visibilityArchiverConfigs.S3store)
	case gcloud.URIScheme:
		if p.visibilityArchiverConfigs.Gstorage == nil {
			return nil, ErrArchiverConfigNotFound
		}
		visibilityArchiver, err = gcloud.NewVisibilityArchiver(container, p.visibilityArchiverConfigs.Gstorage)

	default:
		return nil, ErrUnknownScheme
	}
	if err != nil {
		return nil, err
	}

	p.Lock()
	defer p.Unlock()
	if existingVisibilityArchiver, ok := p.visibilityArchivers[archiverKey]; ok {
		return existingVisibilityArchiver, nil
	}
	p.visibilityArchivers[archiverKey] = visibilityArchiver
	return visibilityArchiver, nil

}

func (p *archiverProvider) getArchiverKey(scheme, serviceName string) string {
	return scheme + ":" + serviceName
}
