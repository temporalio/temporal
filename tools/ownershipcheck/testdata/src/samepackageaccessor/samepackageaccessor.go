// samepackageaccessor: an accessor returns the live map by reference and the
// handler embeds it into a Describe response, all within one package. Inference
// marks the accessor's result borrowed (it does `return v.field`) and flags the
// embed. (The #10706 shape.)
package samepackageaccessor

type Payload struct{}

type Memo struct {
	Fields map[string]*Payload
}

func (*Memo) ProtoReflect() any { return nil }

type DescribeScheduleResponse struct {
	Memo *Memo
}

func (*DescribeScheduleResponse) ProtoReflect() any { return nil }

type Visibility struct {
	memo map[string]*Payload
}

// CustomMemo returns the component's live map by reference (inferred borrowed).
func (v *Visibility) CustomMemo() map[string]*Payload { // want CustomMemo:`borrowed result`
	return v.memo
}

type handler struct {
	vis *Visibility
}

func (h *handler) DescribeSchedule() *DescribeScheduleResponse {
	memo := h.vis.CustomMemo()
	return &DescribeScheduleResponse{
		Memo: &Memo{Fields: memo}, // want `borrowed map embedded into`
	}
}

// DescribeFixed is the fixed form: clone severs the alias, so it must stay silent.
func (h *handler) DescribeFixed() *DescribeScheduleResponse {
	return &DescribeScheduleResponse{
		Memo: &Memo{Fields: cloneMap(h.vis.CustomMemo())},
	}
}

func cloneMap(m map[string]*Payload) map[string]*Payload {
	out := make(map[string]*Payload, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
