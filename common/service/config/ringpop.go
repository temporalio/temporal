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

package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/discovery"
	"github.com/uber/ringpop-go/discovery/jsonfile"
	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/ringpop-go/swim"
	tcg "github.com/uber/tchannel-go"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

const (
	// BootstrapModeNone represents a bootstrap mode set to nothing or invalid
	BootstrapModeNone BootstrapMode = iota
	// BootstrapModeFile represents a file-based bootstrap mode
	BootstrapModeFile
	// BootstrapModeHosts represents a list of hosts passed in the configuration
	BootstrapModeHosts
	// BootstrapModeCustom represents a custom bootstrap mode
	BootstrapModeCustom
)

const (
	defaultMaxJoinDuration = 10 * time.Second
)

// CadenceServices indicate the list of cadence services
var CadenceServices = []string{
	common.FrontendServiceName,
	common.HistoryServiceName,
	common.MatchingServiceName,
	common.WorkerServiceName,
}

// RingpopFactory implements the RingpopFactory interface
type RingpopFactory struct {
	config      *Ringpop
	logger      bark.Logger
	serviceName string
}

// NewFactory builds a ringpop factory conforming
// to the underlying configuration
func (rpConfig *Ringpop) NewFactory(logger bark.Logger, serviceName string) (*RingpopFactory, error) {
	return newRingpopFactory(rpConfig, logger, serviceName)
}

func (rpConfig *Ringpop) validate() error {
	if len(rpConfig.Name) == 0 {
		return fmt.Errorf("ringpop config missing `name` param")
	}
	return validateBootstrapMode(rpConfig)
}

// UnmarshalYAML is called by the yaml package to convert
// the config YAML into a BootstrapMode.
func (m *BootstrapMode) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	var err error
	*m, err = parseBootstrapMode(s)
	return err
}

// parseBootstrapMode reads a string value and returns a bootstrap mode.
func parseBootstrapMode(s string) (BootstrapMode, error) {
	switch strings.ToLower(s) {
	case "hosts":
		return BootstrapModeHosts, nil
	case "file":
		return BootstrapModeFile, nil
	case "custom":
		return BootstrapModeCustom, nil
	}
	return BootstrapModeNone, errors.New("invalid or no ringpop bootstrap mode")
}

func validateBootstrapMode(rpConfig *Ringpop) error {
	switch rpConfig.BootstrapMode {
	case BootstrapModeFile:
		if len(rpConfig.BootstrapFile) == 0 {
			return fmt.Errorf("ringpop config missing bootstrap file param")
		}
	case BootstrapModeHosts:
		if len(rpConfig.BootstrapHosts) == 0 {
			return fmt.Errorf("ringpop config missing boostrap hosts param")
		}
	case BootstrapModeCustom:
		if rpConfig.DiscoveryProvider == nil {
			return fmt.Errorf("ringpop bootstrapMode is set to custom but discoveryProvider is nil")
		}
	default:
		return fmt.Errorf("ringpop config with unknown boostrap mode")
	}
	return nil
}

func newRingpopFactory(rpConfig *Ringpop, logger bark.Logger, serviceName string) (*RingpopFactory, error) {
	if err := rpConfig.validate(); err != nil {
		return nil, err
	}
	if rpConfig.MaxJoinDuration == 0 {
		rpConfig.MaxJoinDuration = defaultMaxJoinDuration
	}
	return &RingpopFactory{config: rpConfig, logger: logger, serviceName: serviceName}, nil
}

// Create is the implementation for MembershipMonitorFactory.Create
func (factory *RingpopFactory) Create(dispatcher *yarpc.Dispatcher) (membership.Monitor, error) {
	// use actual listen port (in case service is bound to :0 or 0.0.0.0:0)
	rp, err := factory.createRingpop(dispatcher)
	if err != nil {
		return nil, fmt.Errorf("ringpop creation failed: %v", err)
	}

	labels, err := rp.Labels()
	if err != nil {
		return nil, fmt.Errorf("ringpop get node labels failed: %v", err)
	}

	if err = labels.Set(membership.RoleKey, factory.serviceName); err != nil {
		return nil, fmt.Errorf("ringpop setting role label failed: %v", err)
	}

	membershipMonitor := membership.NewRingpopMonitor(CadenceServices, rp, factory.logger)
	if err = membershipMonitor.Start(); err != nil {
		return nil, err
	}
	return membershipMonitor, nil
}

func (factory *RingpopFactory) createRingpop(dispatcher *yarpc.Dispatcher) (*ringpop.Ringpop, error) {
	var ch *tcg.Channel
	var err error
	if ch, err = factory.getChannel(dispatcher); err != nil {
		return nil, err
	}

	rp, err := ringpop.New(factory.config.Name, ringpop.Channel(ch))
	if err != nil {
		return nil, err
	}

	discoveryProvider, err := newDiscoveryProvider(factory.config)
	if err != nil {
		return nil, err
	}

	bootstrapOpts := &swim.BootstrapOptions{
		MaxJoinDuration:  factory.config.MaxJoinDuration,
		DiscoverProvider: discoveryProvider,
	}

	_, err = rp.Bootstrap(bootstrapOpts)
	if err != nil {
		return nil, err
	}
	return rp, nil
}

func (factory *RingpopFactory) getChannel(dispatcher *yarpc.Dispatcher) (*tcg.Channel, error) {
	t := dispatcher.Inbounds()[0].Transports()[0].(*tchannel.ChannelTransport)
	ty := reflect.ValueOf(t.Channel())
	var ch *tcg.Channel
	var ok bool
	if ch, ok = ty.Interface().(*tcg.Channel); !ok {
		return nil, errors.New("Unable to get tchannel out of the dispatcher")
	}
	return ch, nil
}

func newDiscoveryProvider(cfg *Ringpop) (discovery.DiscoverProvider, error) {

	if cfg.DiscoveryProvider != nil {
		// custom discovery provider takes first precedence
		return cfg.DiscoveryProvider, nil
	}

	switch cfg.BootstrapMode {
	case BootstrapModeHosts:
		return statichosts.New(cfg.BootstrapHosts...), nil
	case BootstrapModeFile:
		return jsonfile.New(cfg.BootstrapFile), nil
	}
	return nil, fmt.Errorf("unknown bootstrap mode")
}
