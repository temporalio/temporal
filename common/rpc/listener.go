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

package rpc

import (
	"errors"
	"fmt"
	"net"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
)

var ErrStartListener = errors.New("failed to start gRPC listener")

// StartServiceListener starts a gRPC listener on the configured port. The logger and serviceName are just used for
// logging.
func StartServiceListener(rpcCfg *config.RPC, logger log.Logger, serviceName primitives.ServiceName) (net.Listener, error) {
	hostAddress := net.JoinHostPort(getListenIP(rpcCfg, logger).String(), convert.IntToString(rpcCfg.GRPCPort))

	listener, err := net.Listen("tcp", hostAddress)
	if err != nil {
		return nil, fmt.Errorf("can't start %q service: %w: %+v", serviceName, ErrStartListener, err)
	}

	logger.Info("Created gRPC listener", tag.Service(serviceName), tag.Address(hostAddress))

	return listener, nil
}
