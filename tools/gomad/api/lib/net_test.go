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

package lib_test

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

var tcpAddr = "localhost:1234"

func TestListenAndDial(t *testing.T) {
	testutil.StressRun(func(seed int64) {
		ln, err := SIMLIB.Listen("tcp", tcpAddr)
		require.NoError(t, err)

		SIMLANG.Go(func() {
			conn, err := ln.Accept()
			if errors.Is(err, net.ErrClosed) {
				return
			}
			require.NoError(t, err)

			n, err := conn.Write([]byte("ping\n"))
			require.Equal(t, 5, n)
			require.NoError(t, err)

			resp := make([]byte, 5)
			bytesRead, err := conn.Read(resp)
			require.NoError(t, err)
			require.Equal(t, 5, bytesRead)
			require.Equal(t, "pong\n", string(resp))
		})

		d := net.Dialer{}
		conn, err := SIMLIB.Dialer_Dial(&d, "tcp", tcpAddr)
		require.NoError(t, err)

		resp := make([]byte, 5)
		bytesRead, err := conn.Read(resp)
		require.NoError(t, err)
		require.Equal(t, 5, bytesRead)
		require.Equal(t, "ping\n", string(resp))

		n, err := conn.Write([]byte("pong\n"))
		require.Equal(t, 5, n)
		require.NoError(t, err)
	})
}

func TestDialErr(t *testing.T) {
	testutil.StressRun(func(seed int64) {
		_, err := SIMLIB.Dial("tcp", tcpAddr)
		require.ErrorContains(t, err, "connection failed")
	})
}

// TODO: Listen Close
// TODO: Dial Close
// TODO: timeout
