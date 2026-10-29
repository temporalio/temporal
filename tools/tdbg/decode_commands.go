package tdbg

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/codec"
	"google.golang.org/protobuf/encoding/prototext"
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

	message, err := unmarshalProtoByTypeName(protoType, protoData)
	if err != nil {
		return err
	}

	encoder := codec.NewJSONPBIndentEncoder(" ")
	json, err := encoder.Encode(message)
	if err != nil {
		err := fmt.Errorf("unable to encode to JSON: %s", err)
		text, terr := prototext.Marshal(message)
		if terr != nil {
			return err
		}
		fmt.Fprintln(c.App.Writer, err)
		fmt.Fprintln(c.App.Writer, "marshal to text:")
		json = text
	}
	fmt.Fprintln(c.App.Writer, string(json))
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

	fmt.Fprintln(c.App.Writer)
	fmt.Fprintln(c.App.Writer, string(data))
	return nil
}
