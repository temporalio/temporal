package log

type (
	// Config contains the config items for logger
	Config struct {
		// Stdout is true if the output needs to goto standard out; default is stderr
		Stdout bool `yaml:"stdout"`
		// Level is the desired log level; see colocated zap_logger.go::parseZapLevel()
		Level string `yaml:"level"`
		// OutputFile is the path to the log output file
		OutputFile string `yaml:"outputFile"`
		// Format determines the format of each log file printed to the output.
		// Acceptable values are "json" or "console". The default is "json".
		// Use "console" if you want stack traces to appear on multiple lines.
		Format string `yaml:"format"`
		// Development determines whether the logger is run in Development (== Test) or in
		// Production mode.  Default is Production.  Production-stage disables panics from
		// DPanic logging.
		Development bool `yaml:"development"`
	}
)
