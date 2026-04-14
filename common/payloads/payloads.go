package payloads

import (
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

var (
	defaultDataConverter = converter.GetDefaultDataConverter()
)

func EncodeString(str string) *commonpb.Payloads {
	// Error can be safely ignored here becase string always can be converted.
	ps, _ := defaultDataConverter.ToPayloads(str)
	return ps
}

func EncodeInt(i int) *commonpb.Payloads {
	// Error can be safely ignored here becase int always can be converted.
	ps, _ := defaultDataConverter.ToPayloads(i)
	return ps
}

func EncodeBytes(bytes []byte) *commonpb.Payloads {
	// Error can be safely ignored here becase []byte always can be raw encoded.
	ps, _ := defaultDataConverter.ToPayloads(bytes)
	return ps
}

func Encode(value ...any) (*commonpb.Payloads, error) {
	return defaultDataConverter.ToPayloads(value...)
}

func EncodeSingle(value any) (*commonpb.Payload, error) {
	ps, err := defaultDataConverter.ToPayloads(value)
	if err != nil {
		return nil, err
	}
	if len(ps.GetPayloads()) < 1 {
		return nil, nil
	}
	return ps.GetPayloads()[0], nil
}

func MustEncodeSingle(value any) *commonpb.Payload {
	p, err := EncodeSingle(value)
	if err != nil {
		// TODO: nolint
		panic(fmt.Sprintf("unable to encode single payload: %v", err))
	}
	return p
}

func MustEncode(value ...any) *commonpb.Payloads {
	p, err := defaultDataConverter.ToPayloads(value...)
	if err != nil {
		// TODO: nolint
		panic(fmt.Sprintf("unable to encode payloads: %v", err))
	}
	return p
}

func Decode(ps *commonpb.Payloads, valuePtr ...any) error {
	return defaultDataConverter.FromPayloads(ps, valuePtr...)
}

func ToString(ps *commonpb.Payloads) string {
	return fmt.Sprintf("[%s]", strings.Join(defaultDataConverter.ToStrings(ps), ", "))
}
