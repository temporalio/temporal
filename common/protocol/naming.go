package protocol

import (
	"fmt"
	"strings"

	protocolpb "go.temporal.io/api/protocol/v1"
)

type (
	// Type is a protocol type of which there may be many instances like the
	// update protocol or the query protocol.
	Type string

	// MessageType is the type of a message within a protocol.
	MessageType string

	constErr string
)

const (
	MessageTypeUnknown = MessageType("__message_type_unknown")
	TypeUnknown        = Type("__protocol_type_unknown")

	errNilMsg   = constErr("nil message")
	errNilBody  = constErr("nil message body")
	errProtoFmt = constErr("failed to extract protocol type")
	errNoName   = constErr("no message name specified")
)

// String transforms a MessageType into a string
func (mt MessageType) String() string {
	return string(mt)
}

// String tranforms a Type into a string
func (pt Type) String() string {
	return string(pt)
}

// Identify is a function that given a protocol message gives the specific
// message type of the body and the type of the protocol to which this message
// belongs.
func Identify(msg *protocolpb.Message) (Type, MessageType, error) {
	if msg == nil {
		return TypeUnknown, MessageTypeUnknown, errNilMsg
	} else if msg.Body == nil {
		return TypeUnknown, MessageTypeUnknown, errNilBody
	}

	bodyTypeName := string(msg.Body.MessageName())
	if bodyTypeName == "" {
		return TypeUnknown, MessageTypeUnknown, errNoName
	}

	msgType := MessageType(bodyTypeName)
	lastDot := strings.LastIndex(bodyTypeName, ".")
	if lastDot < 0 {
		err := fmt.Errorf("%w: no . found in %q", errProtoFmt, bodyTypeName)
		return TypeUnknown, msgType, err
	}
	return Type(bodyTypeName[0:lastDot]), msgType, nil
}

// IdentifyOrUnknown wraps Identify to return TypeUnknown and/or
// MessageTypeUnknown in the case where either one cannot be determined due to
// an error.
func IdentifyOrUnknown(msg *protocolpb.Message) (Type, MessageType) {
	pt, mt, _ := Identify(msg)
	return pt, mt
}

func (cerr constErr) Error() string {
	return string(cerr)
}
