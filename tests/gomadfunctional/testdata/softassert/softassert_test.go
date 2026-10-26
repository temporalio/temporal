package softassertfixture

import (
	"flag"
	"os"
	"testing"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var mode = flag.String("assertion-mode", "late", "fixture assertion timing")

func fixtureLogger() log.Logger {
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	return log.NewZapLogger(zap.New(zapcore.NewCore(encoder, zapcore.AddSync(os.Stderr), zap.ErrorLevel)))
}

func TestPassingGuestWithOwnLogger(t *testing.T) {
	if *mode == "own" {
		softassert.That(fixtureLogger(), false, "invalid fixture state", tag.String("dynamic", time.Now().String()))
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if *mode == "late" {
		done := make(chan struct{})
		go func() {
			softassert.Fail(fixtureLogger(), "invalid fixture state", tag.String("dynamic", time.Now().String()))
			close(done)
		}()
		<-done
	}
	os.Exit(code)
}
