// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
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

package plugin

import (
	"fmt"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type DataConverterRPC struct {
	client *rpc.Client
}

func NewDataConverterPlugin(name string) (converter.DataConverter, error) {
	client, err := newPluginClient(DataConverterPluginType, name)
	if err != nil {
		return nil, fmt.Errorf("unable to register plugin: %w", err)
	}

	dataConverter, ok := client.(converter.DataConverter)
	if !ok {
		return nil, fmt.Errorf("constructed plugin client type %T doesn't implement converter.DataConverter interface", client)
	}

	return dataConverter, nil
}

func (g *DataConverterRPC) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	err := g.client.Call("Plugin.FromPayload", payload, valuePtr)
	if err != nil {
		return err
	}

	return nil
}

func (g *DataConverterRPC) FromPayloads(payloads *commonpb.Payloads, valuePtr ...interface{}) error {
	err := g.client.Call("Plugin.FromPayloads", payloads, valuePtr)
	if err != nil {
		return err
	}

	return nil
}

func (g *DataConverterRPC) ToPayload(value interface{}) (*commonpb.Payload, error) {
	var payload commonpb.Payload
	err := g.client.Call("Plugin.ToPayload", value, &payload)
	if err != nil {
		return nil, err
	}

	return &payload, nil
}

func (g *DataConverterRPC) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	var payloads commonpb.Payloads
	err := g.client.Call("Plugin.ToPayloads", values, &payloads)
	if err != nil {
		return nil, err
	}

	return &payloads, nil
}

func (g *DataConverterRPC) ToString(input *commonpb.Payload) string {
	var resp string
	err := g.client.Call("Plugin.ToString", input, &resp)
	if err != nil {
		return err.Error()
	}

	return resp
}

func (g *DataConverterRPC) ToStrings(input *commonpb.Payloads) []string {
	var resp []string
	err := g.client.Call("Plugin.ToStrings", input, &resp)
	if err != nil {
		return []string{err.Error()}
	}

	return resp
}

type DataConverterRPCServer struct {
	Impl converter.DataConverter
}

func (s *DataConverterRPCServer) FromPayload(input *commonpb.Payload, resp *interface{}) error {
	var result interface{}
	err := s.Impl.FromPayload(input, result)
	resp = &result
	return err
}

func (s *DataConverterRPCServer) FromPayloads(input *commonpb.Payloads, resp *[]interface{}) error {
	var results []interface{}
	err := s.Impl.FromPayloads(input, results)
	resp = &results
	return err
}

func (s *DataConverterRPCServer) ToPayload(value interface{}, resp *commonpb.Payload) error {
	resp, err := s.Impl.ToPayload(value)
	return err
}

func (s *DataConverterRPCServer) ToPayloads(values []interface{}, resp *commonpb.Payloads) error {
	resp, err := s.Impl.ToPayloads(values)
	return err
}

func (s *DataConverterRPCServer) ToString(input *commonpb.Payload, resp *string) error {
	*resp = s.Impl.ToString(input)
	return nil
}

func (s *DataConverterRPCServer) ToStrings(input *commonpb.Payloads, resp *[]string) error {
	*resp = s.Impl.ToStrings(input)
	return nil
}

type DataConverterPlugin struct {
	Impl converter.DataConverter
}

func (p *DataConverterPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &DataConverterRPCServer{Impl: p.Impl}, nil
}

func (DataConverterPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &DataConverterRPC{client: c}, nil
}
