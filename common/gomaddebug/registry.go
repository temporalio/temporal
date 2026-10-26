package gomaddebug

import "encoding/binary"

const (
	maxEntries   = 256
	maxName      = 128
	emitCapacity = 16 << 10
	dumpCapacity = 64 << 10
	kindDump     = 3
	kindCheck    = 4
	kindProbe    = 5
)

type entry struct {
	kind  uint32
	name  string
	flags uint32
	dump  func(*DumpWriter)
	check func() bool
}

type registry struct {
	entries   [maxEntries]entry
	count     uint32
	closed    bool
	failed    bool
	mask      [4]uint64
	buffer    [emitCapacity]byte
	output    [16 + dumpCapacity]byte
	writer    DumpWriter
	dropped   uint64
	send      func([]byte)
	onFailure func()
}

func (r *registry) declare(kind uint32, name string, flags uint32, dump func(*DumpWriter), check func() bool) uint32 {
	if r.closed || r.failed {
		r.refuse("gomaddebug: registration is closed or incomplete")
	}
	if len(name) == 0 || len(name) > maxName {
		r.refuse("gomaddebug: invalid registration name")
	}
	for i := range len(name) {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' || c == '/') {
			r.refuse("gomaddebug: invalid registration name")
		}
	}
	for i := uint32(0); i < r.count; i++ {
		if r.entries[i].name == name {
			r.refuse("gomaddebug: duplicate registration name")
		}
	}
	if r.count == maxEntries {
		r.refuse("gomaddebug: registry capacity exceeded")
	}
	if kind == kindDump && dump == nil || kind == kindCheck && check == nil {
		r.refuse("gomaddebug: nil registration callback")
	}
	id := r.count
	r.entries[id] = entry{kind: kind, name: name, flags: flags, dump: dump, check: check}
	r.count++
	return id
}

func (r *registry) refuse(reason string) {
	r.failed = true
	if r.onFailure != nil {
		r.onFailure()
	}
	panic(reason)
}
func (r *registry) close() { r.closed = true }

func header(p []byte, kind, id, status, size uint32) {
	binary.LittleEndian.PutUint32(p[0:4], kind)
	binary.LittleEndian.PutUint32(p[4:8], id)
	binary.LittleEndian.PutUint32(p[8:12], status)
	binary.LittleEndian.PutUint32(p[12:16], size)
}

func (r *registry) emit(id uint32, fields []Field) {
	size := 16
	for i := range fields {
		f := &fields[i]
		n := 8
		if f.kind == 3 {
			n = 1
		} else if f.kind == 5 {
			n = len(f.text)
		}
		if f.kind < 1 || f.kind > 5 || n > emitCapacity-size-5 {
			r.drop()
			return
		}
		size += 5 + n
	}
	header(r.buffer[:], 1, id, uint32(len(fields)), uint32(size-16))
	pos := 16
	for i := range fields {
		f := &fields[i]
		n := 8
		if f.kind == 3 {
			n = 1
		} else if f.kind == 5 {
			n = len(f.text)
		}
		r.buffer[pos] = f.kind
		binary.LittleEndian.PutUint32(r.buffer[pos+1:pos+5], uint32(n))
		pos += 5
		switch f.kind {
		case 3:
			r.buffer[pos] = byte(f.bits)
		case 5:
			copy(r.buffer[pos:pos+n], f.text)
		default:
			binary.LittleEndian.PutUint64(r.buffer[pos:pos+8], f.bits)
		}
		pos += n
	}
	r.send(r.buffer[:size])
}

func (r *registry) drop() {
	r.dropped++
	header(r.buffer[:], 4, 0, 0, 8)
	binary.LittleEndian.PutUint64(r.buffer[16:24], r.dropped)
	r.send(r.buffer[:24])
}

func (r *registry) inspect(kind, id uint32) {
	if id >= r.count || r.entries[id].kind != kind {
		panic("gomaddebug: invalid inspection command")
	}
	e := &r.entries[id]
	switch kind {
	case kindDump:
		r.writer.n, r.writer.truncated = 0, false
		status := r.runDump(e.dump)
		if status == 0 && r.writer.truncated {
			status = 2
		}
		header(r.output[:], 2, id, status, uint32(r.writer.n))
		copy(r.output[16:], r.writer.bytes[:r.writer.n])
		r.send(r.output[:16+r.writer.n])
	case kindCheck:
		header(r.output[:], 3, id, runCheck(e.check), 0)
		r.send(r.output[:16])
	default:
		panic("gomaddebug: invalid inspection kind")
	}
}

func (r *registry) runDump(f func(*DumpWriter)) (status uint32) {
	status = 1
	defer func() { _ = recover() }()
	f(&r.writer)
	return 0
}

func runCheck(f func() bool) (status uint32) {
	status = 2
	defer func() { _ = recover() }()
	if !f() {
		return 1
	}
	return 0
}
