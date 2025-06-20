package protoutils

import (
	"reflect"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func UnmarshalAny[T proto.Message](t require.TestingT, a *anypb.Any) T {
	if th, ok := t.(interface{ Helper() }); ok {
		th.Helper()
	}
	pb := new(T)
	ppb := reflect.ValueOf(pb).Elem()
	pbNew := reflect.New(reflect.TypeOf(pb).Elem().Elem())
	ppb.Set(pbNew)

	require.NoError(t, a.UnmarshalTo(*pb))
	return *pb
}

func MarshalAny(t require.TestingT, pb proto.Message) *anypb.Any {
	if th, ok := t.(interface{ Helper() }); ok {
		th.Helper()
	}
	a, err := anypb.New(pb)
	require.NoError(t, err)
	return a
}
