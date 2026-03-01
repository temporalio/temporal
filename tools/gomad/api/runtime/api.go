package runtime

import (
	"go.temporal.io/server/tools/gomad/runtime"

	// forces packages to be found in transformation
	_ "go.temporal.io/server/tools/gomad/api/lang"
	_ "go.temporal.io/server/tools/gomad/api/lib"

	// force external packages to be included
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/bisect"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/cfg"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/database/sql"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/database/sql/driver"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc/otlpmetric"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc/otlptrace"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/godebug"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/godebugs"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/net/http"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/net/http/ascii"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/net/http/httptest"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/net/http/httputil"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/net/http/pprof"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/nettrace"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/platform"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/safefilepath"
	_ "go.temporal.io/server/tools/gomad/api/ext-lib/testenv"
)

type Info = sim_runtime.Info

var Hook = sim_runtime.Hook
var Start = sim_runtime.Start
var Join = sim_runtime.Join
var DebugMode = sim_runtime.DebugMode
var Seed = sim_runtime.Seed
var Logger = sim_runtime.Logger
