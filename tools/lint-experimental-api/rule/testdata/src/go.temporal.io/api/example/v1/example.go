package example

type Request struct {
	NewField    string
	StableField string
}

func (*Request) GetNewField() string {
	return ""
}

type Status int32

const (
	STATUS_UNSPECIFIED  Status = 0
	STATUS_EXPERIMENTAL Status = 1
)
