package okx

// Logger is the minimal structured-logging surface the SDK depends on. It is
// deliberately compatible with the shape of log/slog.Logger's leveled methods,
// so *slog.Logger satisfies it directly.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// noopLogger is the zero-cost default; all methods are inlined no-ops.
type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}
