package interceptor

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	maxMessageLength = 4000
	truncatedSuffix  = "... <truncated>"
)

func ServiceErrorInterceptor(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {

	resp, err := handler(ctx, req)

	var deserializationError *serialization.DeserializationError
	var serializationError *serialization.SerializationError
	// convert serialization errors to be captured as serviceerrors across gRPC calls
	if errors.As(err, &deserializationError) || errors.As(err, &serializationError) {
		err = serviceerror.NewDataLoss(err.Error())
	}

	// truncate message length if needed
	st := serviceerror.ToStatus(err)
	if len(st.Message()) > maxMessageLength {
		p := st.Proto()
		p.Message = util.TruncateUTF8(p.Message, maxMessageLength-len(truncatedSuffix)) + truncatedSuffix
		st = status.FromProto(p)
	}

	return resp, st.Err()
}
