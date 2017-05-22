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
	"fmt"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"log"
	"net"
	"strings"
)

// TChannelFactory is an implementation of
// service.TChannelFactory interface
type TChannelFactory struct {
	config *TChannel
}

// NewFactory builds a new tchannelFactory
// conforming to the underlying configuration
func (cfg *TChannel) NewFactory() *TChannelFactory {
	return newTChannelFactory(cfg)
}

func newTChannelFactory(cfg *TChannel) *TChannelFactory {
	factory := &TChannelFactory{config: cfg}
	return factory
}

// CreateChannel implements the TChannelFactory interface
func (factory *TChannelFactory) CreateChannel(sName string,
	thriftServices []thrift.TChanServer) (*tchannel.Channel, *thrift.Server) {

	opts := &tchannel.ChannelOptions{
		Logger: tchannel.NullLogger,
	}

	if !factory.config.DisableLogging {
		level := factory.config.getLogLevel()
		opts.Logger = tchannel.NewLevelLogger(tchannel.SimpleLogger, level)
	}

	ch, err := tchannel.NewChannel(sName, opts)
	if err != nil {
		log.Fatalf("tchannel.NewChannel() failed, err=%v", err)
	}

	server := thrift.NewServer(ch)
	for _, s := range thriftServices {
		server.Register(s)
	}

	factory.listenAndServe(ch)

	return ch, server
}

func (factory *TChannelFactory) listenAndServe(ch *tchannel.Channel) {
	ip := factory.getListenIP()
	err := ch.ListenAndServe(fmt.Sprintf("%v:%v", ip, factory.config.Port))
	if err != nil {
		log.Fatalf("tchannel.ListenAndServer failed, err=%v", err)
	}
}

func (factory *TChannelFactory) getListenIP() net.IP {
	if factory.config.BindOnLocalHost {
		return net.IPv4(127, 0, 0, 1)
	}
	ip, err := tchannel.ListenIP()
	if err != nil {
		log.Fatalf("tchannel.ListenIP failed, err=%v", err)
	}
	return ip
}

func (cfg *TChannel) getLogLevel() tchannel.LogLevel {
	return cfg.parseLogLevel()
}

// parseLogLevel converts the string log level
// into tchannel logger level
func (cfg *TChannel) parseLogLevel() tchannel.LogLevel {
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		return tchannel.LogLevelDebug
	case "info":
		return tchannel.LogLevelInfo
	case "warn":
		return tchannel.LogLevelWarn
	case "error":
		return tchannel.LogLevelError
	case "fatal":
		return tchannel.LogLevelFatal
	default:
		return tchannel.LogLevelWarn
	}
}
