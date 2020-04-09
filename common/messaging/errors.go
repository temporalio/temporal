package messaging

import "errors"

var (
	// ErrMessageSizeLimit indicate that message is rejected by server due to size limitation
	ErrMessageSizeLimit = errors.New("message was too large, server rejected it to avoid allocation error")
)
