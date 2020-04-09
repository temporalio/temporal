package codec

import (
	"bytes"
	"encoding/json"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	eventpb "go.temporal.io/temporal-proto/event"
)

type (
	// JSONPBEncoder is JSON encoder/decoder for protobuf structs and slices of protobuf structs.
	// This is an wrapper on top of jsonpb.Marshaler which supports not only single object serialization
	// but also slices of concrete objects.
	JSONPBEncoder struct {
		marshaler   jsonpb.Marshaler
		ubmarshaler jsonpb.Unmarshaler
	}
)

// NewJSONPBEncoder creates a new JSONPBEncoder.
func NewJSONPBEncoder() *JSONPBEncoder {
	return &JSONPBEncoder{
		marshaler:   jsonpb.Marshaler{},
		ubmarshaler: jsonpb.Unmarshaler{},
	}
}

// NewJSONPBIndentEncoder creates a new JSONPBEncoder with indent.
func NewJSONPBIndentEncoder(indent string) *JSONPBEncoder {
	return &JSONPBEncoder{
		marshaler:   jsonpb.Marshaler{Indent: indent},
		ubmarshaler: jsonpb.Unmarshaler{},
	}
}

// Encode protobuf struct to bytes.
func (e *JSONPBEncoder) Encode(pb proto.Message) ([]byte, error) {
	var buf bytes.Buffer
	err := e.marshaler.Marshal(&buf, pb)
	return buf.Bytes(), err
}

// Decode bytes to protobuf struct.
func (e *JSONPBEncoder) Decode(data []byte, pb proto.Message) error {
	return e.ubmarshaler.Unmarshal(bytes.NewReader(data), pb)
}

// Encode HistoryEvent slice to bytes.
func (e *JSONPBEncoder) EncodeHistoryEvents(historyEvents []*eventpb.HistoryEvent) ([]byte, error) {
	return e.encodeSlice(
		len(historyEvents),
		func(i int) proto.Message { return historyEvents[i] })
}

// Encode History slice to bytes.
func (e *JSONPBEncoder) EncodeHistories(histories []*eventpb.History) ([]byte, error) {
	return e.encodeSlice(
		len(histories),
		func(i int) proto.Message { return histories[i] })
}

// Decode HistoryEvent slice from bytes.
func (e *JSONPBEncoder) DecodeHistoryEvents(data []byte) ([]*eventpb.HistoryEvent, error) {
	var historyEvents []*eventpb.HistoryEvent
	err := e.decodeSlice(
		data,
		func() proto.Message {
			historyEvent := &eventpb.HistoryEvent{}
			historyEvents = append(historyEvents, historyEvent)
			return historyEvent
		})
	return historyEvents, err
}

// Decode History slice from bytes.
func (e *JSONPBEncoder) DecodeHistories(data []byte) ([]*eventpb.History, error) {
	var histories []*eventpb.History
	err := e.decodeSlice(
		data,
		func() proto.Message {
			history := &eventpb.History{}
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
		if err := e.marshaler.Marshal(&buf, pb); err != nil {
			return nil, err
		}

		if i == len-1 {
			buf.WriteString("]")
		} else {
			buf.WriteString(",")
		}
	}
	return buf.Bytes(), nil
}

// constructor callback must create empty object, add it to result slice, and return it.
func (e *JSONPBEncoder) decodeSlice(
	data []byte,
	constructor func() proto.Message) error {
	jsonDecoder := json.NewDecoder(bytes.NewReader(data))

	_, err := jsonDecoder.Token() // Read leading `[` and ignore it
	if err != nil {
		return err
	}
	for jsonDecoder.More() {
		pb := constructor()
		err := jsonpb.UnmarshalNext(jsonDecoder, pb)
		if err != nil {
			return err
		}
	}

	return nil
}
