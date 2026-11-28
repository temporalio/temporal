package faultinjection

// Replacement carries the response that a fault substitutes for the result of a call. A failpoint
// returns an error only to fail the call, so a request or a Result that embeds Replacement lets
// the fault answer the call with a response instead: it calls Replace on the argument of its
// execution (the request, or a *Result) and returns no error.
//
// A response that a fault replaces with another, or drops by returning an error, is discarded. The
// caller that owns the responses, such as one that must close the body of an HTTP response, takes
// them with Discarded and releases each once.
type Replacement[Resp any] struct {
	response  Resp
	set       bool
	discarded []Resp
}

// Replace sets the response that replaces the result of the call. It is ignored if the fault also
// returns an error. A response that an earlier call set is discarded.
func (r *Replacement[Resp]) Replace(resp Resp) {
	if r.set {
		r.discarded = append(r.discarded, r.response)
	}
	r.response, r.set = resp, true
}

// Replaced returns the response set with Replace, or the zero value if there is none.
func (r *Replacement[Resp]) Replaced() Resp {
	return r.response
}

// Drop discards the response set with Replace, if any, as the call fails with an error instead.
func (r *Replacement[Resp]) Drop() {
	if r.set {
		var none Resp
		r.discarded = append(r.discarded, r.response)
		r.response, r.set = none, false
	}
}

// Discard adds responses that another Replacement discarded, so the caller that has this one
// takes them too.
func (r *Replacement[Resp]) Discard(responses ...Resp) {
	r.discarded = append(r.discarded, responses...)
}

// Discarded returns the responses that were discarded since the last call, and forgets them. A
// response can appear more than once, if a fault replaced with it more than once.
func (r *Replacement[Resp]) Discarded() []Resp {
	discarded := r.discarded
	r.discarded = nil
	return discarded
}

// Result is the result of a handler that a response fault checks. A fault receives it as a
// *Result.
type Result[Req, Resp any] struct {
	Operation string
	Request   Req
	Response  Resp
	Error     error
	Replacement[Resp]
}
