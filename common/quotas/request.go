package quotas

type (
	Request struct {
		API           string
		Token         int
		Caller        string
		CallerType    string
		CallerSegment int32
		Initiation    string
	}
)

func NewRequest(
	api string,
	token int,
	caller string,
	callerType string,
	callerSegment int32,
	initiation string,
) Request {
	return Request{
		API:           api,
		Token:         token,
		Caller:        caller,
		CallerType:    callerType,
		CallerSegment: callerSegment,
		Initiation:    initiation,
	}
}
