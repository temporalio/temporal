// getterparam negative fixture (M2.1): a proto getter invoked on a PARAMETER
// (not the receiver) returns an owned value — a parameter is optimistically
// owned, so its projected field is too. Must stay silent. Mirrors
// scheduler/migration.go's searchAttributes.GetIndexedFields() on a param.
package getterparam

type Payload struct{}

type SearchAttributes struct {
	IndexedFields map[string]*Payload
}

// GetIndexedFields is a getter returning a receiver field (inferred viaReceiver).
func (x *SearchAttributes) GetIndexedFields() map[string]*Payload { // want GetIndexedFields:`borrowed result`
	return x.IndexedFields
}

type Request struct {
	Fields map[string]*Payload
}

func (*Request) ProtoReflect() any { return nil }

func BuildRequest(sa *SearchAttributes) *Request { // want BuildRequest:`embeds param`
	return &Request{Fields: sa.GetIndexedFields()} // owned here (param); embed is silent
}
