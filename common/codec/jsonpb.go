package codec

import (
	"bytes"
	"encoding/json"
	"fmt"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type (
	// JSONPBEncoder is JSON encoder/decoder for protobuf structs and slices of protobuf structs.
	// This is an wrapper on top of jsonpb.Marshaler which supports not only single object serialization
	// but also slices of concrete objects.
	JSONPBEncoder struct {
		marshaler   protojson.MarshalOptions
		unmarshaler temporalproto.CustomJSONUnmarshalOptions
	}
)

// NewJSONPBEncoder creates a new JSONPBEncoder.
func NewJSONPBEncoder() JSONPBEncoder {
	return JSONPBEncoder{}
}

// NewJSONPBIndentEncoder creates a new JSONPBEncoder with indent.
func NewJSONPBIndentEncoder(indent string) JSONPBEncoder {
	return JSONPBEncoder{
		marshaler: protojson.MarshalOptions{
			Indent: indent,
		},
	}
}

// Encode protobuf struct to bytes.
func (e JSONPBEncoder) Encode(pb proto.Message) ([]byte, error) {
	return e.marshaler.Marshal(pb)
}

// Decode bytes to protobuf struct.
func (e JSONPBEncoder) Decode(data []byte, pb proto.Message) error {
	return e.unmarshaler.Unmarshal(data, pb)
}

// Encode HistoryEvent slice to bytes.
func (e *JSONPBEncoder) EncodeHistoryEvents(historyEvents []*historypb.HistoryEvent) ([]byte, error) {
	return e.encodeSlice(
		len(historyEvents),
		func(i int) proto.Message { return historyEvents[i] })
}

// Encode History slice to bytes.
func (e *JSONPBEncoder) EncodeHistories(histories []*historypb.History) ([]byte, error) {
	return e.encodeSlice(
		len(histories),
		func(i int) proto.Message { return histories[i] })
}

// Decode HistoryEvent slice from bytes.
func (e *JSONPBEncoder) DecodeHistoryEvents(data []byte) ([]*historypb.HistoryEvent, error) {
	var historyEvents []*historypb.HistoryEvent
	err := e.DecodeSlice(
		data,
		func() proto.Message {
			historyEvent := &historypb.HistoryEvent{}
			historyEvents = append(historyEvents, historyEvent)
			return historyEvent
		})
	return historyEvents, err
}

// Decode History slice from bytes.
func (e *JSONPBEncoder) DecodeHistories(data []byte) ([]*historypb.History, error) {
	var histories []*historypb.History
	err := e.DecodeSlice(
		data,
		func() proto.Message {
			history := &historypb.History{}
			histories = append(histories, history)
			return history
		})

	return histories, err
}

// Due to the lack of generics in go
// this function accepts callback which should return particular item by it index.
func (e *JSONPBEncoder) encodeSlice(
	len int,
	item func(i int) proto.Message,
) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := 0; i < len; i++ {
		pb := item(i)
		bs, err := e.marshaler.Marshal(pb)
		if err != nil {
			return nil, err
		}
		buf.Write(bs)

		if i < len-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString("]")
	return buf.Bytes(), nil
}

// constructor callback must create empty object, add it to result slice, and return it.
func (e *JSONPBEncoder) DecodeSlice(
	data []byte,
	constructor func() proto.Message) error {

	dec := json.NewDecoder(bytes.NewReader(data))

	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("invalid json: expected [ but found %v", tok)
	}

	// We need DiscardUnknown here as the history json may have been written by a
	// different proto revision
	unmarshaller := temporalproto.CustomJSONUnmarshalOptions{
		DiscardUnknown: true,
	}

	var buf json.RawMessage
	for dec.More() {
		if err := dec.Decode(&buf); err != nil {
			return err
		}
		pb := constructor()
		if err := unmarshaller.Unmarshal([]byte(buf), pb); err != nil {
			return err
		}
		buf = buf[:0]
	}

	return nil
}
