package persistence

import (
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

const (
	// pageTokenPrefixByte is the first byte of the serialized page token. It's used to ensure that the page token is
	// not empty. Without this, if the last_read_message_id is 0, the serialized page token would be empty, and clients
	// could erroneously assume that there are no more messages beyond the first page. This is purely used to ensure
	// that tokens are non-empty; it is not used to verify that the token is valid like the magic byte in some other
	// protocols.
	pageTokenPrefixByte = 0
)

func GetNextPageToken(result []QueueV2Message) []byte {
	if len(result) == 0 {
		return nil
	}
	lastReadMessageID := result[len(result)-1].MetaData.ID
	token := &persistencespb.ReadQueueMessagesNextPageToken{
		LastReadMessageId: lastReadMessageID,
	}
	// This can never fail if you inspect the implementation.
	b, _ := token.Marshal()

	// See the comment above pageTokenPrefixByte for why we want to do this.
	return append([]byte{pageTokenPrefixByte}, b...)
}

func GetMinMessageID(request *InternalReadMessagesRequest) (int64, error) {
	// TODO: start from the ack level of the queue partition instead of the first message ID when there is no token.
	if len(request.NextPageToken) == 0 {
		return FirstQueueMessageID, nil
	}
	var token persistencespb.ReadQueueMessagesNextPageToken

	// Skip the first byte. See the comment on pageTokenPrefixByte for more details.
	err := token.Unmarshal(request.NextPageToken[1:])
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %q: %v",
			ErrInvalidReadQueueMessagesNextPageToken,
			request.NextPageToken,
			err,
		)
	}
	return token.LastReadMessageId + 1, nil
}
