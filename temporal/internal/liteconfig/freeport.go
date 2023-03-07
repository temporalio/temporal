// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package liteconfig

import (
	"fmt"
	"net"
)

// Modified from https://github.com/phayes/freeport/blob/95f893ade6f232a5f1511d61735d89b1ae2df543/freeport.go

func NewPortProvider() *PortProvider {
	return &PortProvider{}
}

type PortProvider struct {
	listeners []*net.TCPListener
}

// GetFreePort asks the kernel for a free open port that is ready to use.
func (p *PortProvider) GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		if addr, err = net.ResolveTCPAddr("tcp6", "[::1]:0"); err != nil {
			panic(fmt.Sprintf("temporalite: failed to get free port: %v", err))
		}
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	p.listeners = append(p.listeners, l)

	return l.Addr().(*net.TCPAddr).Port, nil
}

func (p *PortProvider) MustGetFreePort() int {
	port, err := p.GetFreePort()
	if err != nil {
		panic(err)
	}
	return port
}

func (p *PortProvider) Close() error {
	for _, l := range p.listeners {
		if err := l.Close(); err != nil {
			return err
		}
	}
	return nil
}
