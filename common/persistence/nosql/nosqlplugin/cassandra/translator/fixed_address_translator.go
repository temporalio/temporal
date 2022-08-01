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

package translator

import (
	"errors"
	"fmt"
	"net"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
)

const (
	fixedTranslatorName   = "fixed-address-translator"
	advertisedHostnameKey = "advertised-hostname"
)

func init() {
	RegisterTranslator(fixedTranslatorName, NewFixedAddressTranslatorPlugin())
}

type FixedAddressTranslatorPlugin struct {
}

func NewFixedAddressTranslatorPlugin() TranslatorPlugin {
	return &FixedAddressTranslatorPlugin{}
}

// GetTranslator What gocql driver does is that it will connect to the first node in the list in configuration
// (if there is more than one), if it fails to connect to it, it will pick another from that list and so on so.
// When the connection is initialised, the driver does not know what the topology of a cluster looks like yet.
// It just connected to a node. In order to know, for the driver itself, internally, what the topology is like,
// it will query that node it just connected to, and it will read system.peers table. In that table,
// there are all other nodes of a cluster as that node, gocql just connected to, sees it.
//
// Every other node has the very same system.peers table where all other nodes of the cluster are.
// The returned rows, representing all other nodes in the cluster, contain IP addresses internal of that cluster.
// They are not necessarily the same hostnames as the ones you would translate with your service resolver
// (they are IP addresses, not hostnames, actually).
//
// For the case if the nodes are behind the proxy, service resolver would translate just the publicly
// visible hostname which a user put into configuration so driver can connect to it, but that is not enough,
// IP addresses behind a proxy are not reachable from client's perspective. These are not translatable
// with service resolver so client can not connect to them directly - that is what gocql address
// translator is exactly for.
//
// The implementation of fixed address translator plugin is fed the internal IP address from the system.peers table,
// and it will always return same fixed ip based on what advertised-hostname is resolved to. That IP address,
// from client's perspective, might be, for example, an IP address of a load balancer which is reachable from client.
// As the IP address of all nodes are some, the difference between the nodes can be achieved by running them on
// a different port for each node.
// see also https://github.com/gocql/gocql/pull/1635
func (plugin *FixedAddressTranslatorPlugin) GetTranslator(cfg *config.Cassandra) (gocql.AddressTranslator, error) {
	if cfg.AddressTranslator == nil {
		return nil, errors.New("there is no addressTranslator configuration in cassandra configuration")
	}

	opts := cfg.AddressTranslator.Options
	if opts == nil {
		return nil, errors.New("there are no options for translator plugin")
	}

	advertisedHostname, found := opts[advertisedHostnameKey]

	if !found || len(advertisedHostname) == 0 {
		return nil, errors.New("detected no advertised-hostname key or empty value for translator plugin options")
	}

	var resolvedIp net.IP = nil
	ips, _ := net.LookupIP(advertisedHostname)
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			resolvedIp = ipv4
			break
		}
	}

	if resolvedIp == nil {
		return nil, fmt.Errorf("no resolved IP for advertised hostname %q", advertisedHostname)
	}

	return gocql.AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		return resolvedIp, port
	}), nil
}
