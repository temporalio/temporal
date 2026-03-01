package sim_runtime

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/rivo/uniseg"
)

var (
	// NOTE: not using zap etc. to prevent cyclic dependency
	Logger   *log.Logger
	logLine  int
	logMutex sync.Mutex
	emptyTag Tag
)

type Tag struct {
	k string
	v any
}

func init() {
	Logger = log.New(os.Stdout, "", 0)
}

func Print(symbol string, msg string, tags ...Tag) {
	Logger.Printf("%v%v%v\n",
		pad(strings.TrimSpace(symbol), 10, true),
		strings.TrimSpace(msg),
		strings.Join(renderTags(tags), " "))
}

func Dbg(symbol string, msg string, tags ...Tag) {
	logMutex.Lock()
	defer func() { logMutex.Unlock() }()

	if CurrentSimulator().scheduler != nil && CurrentSimulator().scheduler.debug {
		var ctx string
		if g := CurrentSimulator().scheduler.active; g != nil {
			ctx = fmt.Sprintf("[#%v]", g.id)
		} else {
			ctx = ""
		}

		Logger.Printf("%v%v%v%v%v\n",
			pad(fmt.Sprintf("[%d]", logLine), 9, false),
			pad(ctx, 9, false),
			pad(strings.TrimSpace(symbol), 10, true),
			pad(strings.TrimSpace(msg), 12, false),
			strings.Join(renderTags(tags), " "))
		logLine += 1
	}
}

func renderTags(tags []Tag) []string {
	args := make([]string, len(tags))
	for i, t := range tags {
		if t.k == "" {
			// means to skip entirely
			continue
		}

		val := t.v
		if reflect.TypeOf(val).Kind() == reflect.Func {
			fn := reflect.ValueOf(val)
			val = fn.Call(nil)[0].String()
		}

		args[i] = fmt.Sprintf("(%v:%v)", t.k, val)
	}
	return args
}

func GoTag(g *goroutine) Tag {
	return Tag{"go", fmt.Sprintf("#%v", g.id)}
}

func ChanTag(id ChannelId) Tag {
	return Tag{"ch", fmt.Sprintf("#%v", id)}
}

func CurLocTag() Tag {
	var loc string
	if CurrentSimulator().debug {
		// this is expensive
		loc = currentSourceLocation()
	}
	return LocTag(loc)
}

func LocTag(loc string) Tag {
	return Tag{"src", loc}
}

func BufTag[T any](ch chan T) Tag {
	if cap(ch) > 0 {
		return Tag{"buf", fmt.Sprintf("%v/%v", len(ch), cap(ch))}
	}
	return emptyTag
}

func TimeTag(k string, v int64) Tag {
	return Tag{k, time.UnixMilli(v).Format(time.RFC3339Nano)}
}

func AnyTag(k string, v any) Tag {
	return Tag{k, v}
}

func pad(s string, minSize int, symbols bool) string {
	var chars int
	if symbols {
		chars = uniseg.GraphemeClusterCount(s) * 2
	} else {
		chars = len(s)
	}

	spacesToAdd := minSize - chars
	if spacesToAdd <= 0 {
		return s
	}
	return s + strings.Repeat(" ", spacesToAdd)
}
