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

package tdbg

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/urfave/cli/v2"

	"go.temporal.io/server/common/codec"
)

func AdminDecodeProto(c *cli.Context) error {
	protoType := c.String(FlagProtoType)

	var protoData []byte
	var err error

	binaryFile := c.String(FlagBinaryFile)
	if binaryFile != "" {
		protoData, err = os.ReadFile(binaryFile)
		if err != nil {
			return fmt.Errorf("unable to read binary file %s: %s", binaryFile, err)
		}
	}

	if protoData == nil {
		hexData := c.String(FlagHexData)
		hexFile := c.String(FlagHexFile)
		if hexData == "" && hexFile != "" {
			hexBytes, err := os.ReadFile(hexFile)
			if err != nil {
				return fmt.Errorf("unable to read hex file %s: %s", hexFile, err)
			}
			hexData = string(hexBytes)
		}

		hexData = strings.TrimPrefix(hexData, "0x")

		if hexData != "" {
			protoData, err = hex.DecodeString(hexData)
			if err != nil {
				cutLen := 10
				dots := "..."
				if len(hexData) <= cutLen {
					cutLen = len(hexData)
					dots = ""
				}
				return fmt.Errorf("unable to decode hex data %s%s: %s", hexData[:cutLen], dots, err)
			}
		}
	}

	if protoData == nil {
		return fmt.Errorf("missing required parameter data flag")
	}

	messageType := proto.MessageType(protoType)
	if messageType == nil {
		return fmt.Errorf("unable to find %s type", protoType)
	}
	message := reflect.New(messageType.Elem()).Interface().(proto.Message)
	err = proto.Unmarshal(protoData, message)
	if err != nil {
		return fmt.Errorf("unable to unmarshal to %s", protoType)
	}

	encoder := codec.NewJSONPBIndentEncoder(" ")
	json, err := encoder.Encode(message)
	if err != nil {
		return fmt.Errorf("unable to encode to JSON: %s", err)
	}
	fmt.Println()
	fmt.Println(string(json))
	return nil
}

func AdminDecodeBase64(c *cli.Context) error {
	base64Data := c.String(FlagBase64Data)
	base64File := c.String(FlagBase64File)
	if base64Data == "" && base64File != "" {
		base64Bytes, err := os.ReadFile(base64File)
		if err != nil {
			return fmt.Errorf("unable to read base64 file %s: %s", base64File, err)
		}
		base64Data = string(base64Bytes)
	}

	if base64Data == "" {
		return fmt.Errorf("no data flag is specified")
	}

	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		cutLen := 10
		dots := "..."
		if len(base64Data) <= cutLen {
			cutLen = len(base64Data)
			dots = ""
		}
		return fmt.Errorf("unable to decode base64 data %s%s: %s", base64Data[:cutLen], dots, err)
	}

	fmt.Println()
	fmt.Println(string(data))
	return nil
}
