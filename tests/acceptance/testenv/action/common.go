package action

import (
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/testing/stamp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Payloads struct{}

// TODO: make random
func (g Payloads) Next(ctx stamp.GenContext) *commonpb.Payloads {
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{Payload{}.Next(ctx)},
	}
}

type Payload struct{}

// TODO: make random
func (g Payload) Next(ctx stamp.GenContext) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			"meta_data_key": []byte(`meta_data_val`),
		},
		Data: []byte(`test data`),
	}
}

type Header struct{}

// TODO: make random
func (g Header) Next(ctx stamp.GenContext) *commonpb.Header {
	return &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"header_field": Payload{}.Next(ctx),
		},
	}
}

func unmarshalAny[T proto.Message](a *anypb.Any) T {
	pb := new(T)
	ppb := reflect.ValueOf(pb).Elem()
	pbNew := reflect.New(reflect.TypeOf(pb).Elem().Elem())
	ppb.Set(pbNew)
	err := a.UnmarshalTo(*pb)
	if err != nil {
		panic(err)
	}
	return *pb
}

func marshalAny(pb proto.Message) *anypb.Any {
	a, err := anypb.New(pb)
	if err != nil {
		panic(err)
	}
	return a
}
