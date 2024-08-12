// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package hsm

// RemoteMethod can be defined for each state machine to handle external request, like RPCs, but as part of the HSM
// framework. See RemoteExecutor for how to define the handler for remote methods.
type RemoteMethod interface {
	// Name of the remote method. Must be unique per state machine.
	Name() string
	// SerializeOutput serializes output of the invocation to a byte array that is suitable for transport.
	SerializeOutput(output any) ([]byte, error)
	// DeserializeInput deserializes input from bytes that is then passed to the handler.
	DeserializeInput(data []byte) (any, error)
}

type remoteMethodDefinition struct {
	method   RemoteMethod
	executor RemoteExecutor
}
